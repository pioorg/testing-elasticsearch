package testing_elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;

@Testcontainers
public class BookSearcherIntTest {

    static final String ELASTICSEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.15.0";
    static final JacksonJsonpMapper JSONP_MAPPER = new JacksonJsonpMapper();

    RestClientTransport transport;
    ElasticsearchClient client;

    @Container
    ElasticsearchContainer elasticsearch = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);

    @BeforeEach
    void setupClient() {
        transport = new RestClientTransport(new ElasticsearchRestClientBuilder()
            .withHttpHost(new HttpHost(elasticsearch.getHost(), elasticsearch.getMappedPort(9200), "https"))
            .withSslContext(elasticsearch.createSslContextFromCa())
            .withUsernameAndPassword("elastic", System.getenv().getOrDefault("ESPSWD", "changeme"))
            .build(), JSONP_MAPPER);
        client = new ElasticsearchClient(transport);
    }

    @AfterEach
    void closeClient() throws IOException {
        if (transport != null) {
            transport.close();
        }
    }


    // Please keep in mind that this way of initialising data for Elasticsearch performs terribly.
    // Please don't do anything like this for your tests.
    // This is merely a starting point for optimisation in subsequent steps.

    @BeforeEach
    void setupDataInContainer() throws IOException, InterruptedException {

        LocalTime started = LocalTime.now();
        System.out.println("Starting up with data initialization " + started);
        // first we need to create the index and give it a precise mapping, just like for production
        client.indices()
            .create(c -> c
                .index("books")
                .mappings(mp -> mp
                    .properties("title", p -> p.text(t -> t))
                    .properties("description", p -> p.text(t -> t))
                    .properties("author", p -> p.text(t -> t))
                    .properties("year", p -> p.short_(s -> s))
                    .properties("publisher", p -> p.text(t -> t))
                    .properties("ratings", p -> p.halfFloat(hf -> hf))
                ));

        // this record class will help up with deserialization of the CSV data we use to initialize Elasticsearch
        @com.fasterxml.jackson.annotation.JsonPropertyOrder({"title", "description", "author", "year", "publisher", "ratings"})
        record Book(
            String title,
            String description,
            String author,
            int year,
            String publisher,
            float ratings
        ) {
        }

        // this is to tell what's the structure of our CSV data
        CsvMapper csvMapper = new CsvMapper();
        CsvSchema schema = csvMapper
            .typedSchemaFor(Book.class)
            .withHeader()
            .withColumnSeparator(';')
            .withSkipFirstDataRow(true);

        // this is where we fetch the data from
        String booksUrl = "https://raw.githubusercontent.com/elastic/elasticsearch-php-examples/main/examples/ESQL/data/books.csv";


        try (HttpClient httpClient = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(booksUrl))
                .build();

            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            // instead of fetching the whole file locally first, we're going to stream it
            InputStream csvContentStream = response.body();

            // from now on we stream the data and add it
            MappingIterator<Book> it = csvMapper
                .readerFor(Book.class)
                .with(schema)
                .readValues(new InputStreamReader(csvContentStream));

            boolean hasNext = false;

            do {
                try {
                    Book book = it.nextValue();
                    client.index(i -> i.index("books").document(book));
                    hasNext = it.hasNextValue();
                } catch (JsonParseException | InvalidFormatException e) {
                    // ignore malformed data
                }
            } while (hasNext);
        }
        // please don't go with Thread.sleep(1_000) "for the data to appear"; instead: refresh
        // more: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html
        client.indices().refresh();
        LocalTime finished = LocalTime.now();
        System.out.println("Finished data initialization " + finished + ", so it took " + Duration.between(started, finished).getSeconds() + " seconds.");
    }

    @Test
    void canCreateClientWithContainerRunning_8_15() {
        Assertions.assertDoesNotThrow(() -> new BookSearcher(client));
    }

    @Test
    void shouldGiveMostPublishedAuthorsInGivenYears() {
        var systemUnderTest = new BookSearcher(client);
        var list = systemUnderTest.mostPublishedAuthorsInYears(1800, 2010);
        Assertions.assertEquals("Beatrix Potter", list.get(12).author(), "Beatrix Potter was 13th most published author between 1800 and 2010");
    }
}
