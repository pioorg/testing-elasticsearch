package testing_elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;

public class BookSearcherIntTest {

    static final String ELASTICSEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.15.0";
    static final JacksonJsonpMapper JSONP_MAPPER = new JacksonJsonpMapper();
    static final Path DATA_PATH = Path.of("src/test/resources/books.ndjson");

    RestClientTransport transport;
    ElasticsearchClient client;

    static ElasticsearchContainer elasticsearch =
        new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
            .withCopyToContainer(MountableFile.forHostPath("src/test/resources/books.ndjson"), "/tmp/books.ndjson");

    @BeforeAll
    static void setupDataIfMissing() throws IOException, InterruptedException {
        if (!Files.exists(DATA_PATH)) {
            try (Writer output = new FileWriter(DATA_PATH.toFile())) {
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
                // new OutputStreamWriter(System.out) can be used for debugging
                CSV2JSONConverter.convert("https://raw.githubusercontent.com/elastic/elasticsearch-php-examples/main/examples/ESQL/data/books.csv", output, Book.class, "books");
            }
        }
        Startables.deepStart(elasticsearch).join();
    }


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

    @BeforeEach
    void setupDataInContainer() throws IOException, InterruptedException {

        LocalTime started = LocalTime.now();
        System.out.println("Starting up with data initialization " + started);
        // first we need to delete an index, in case it still exists
        ExecResult result = elasticsearch.execInContainer(
            "curl", "https://localhost:9200/books", "-u", "elastic:changeme",
            "--cacert", "/usr/share/elasticsearch/config/certs/http_ca.crt",
            "-X", "DELETE"
        );
        // we don't check the result, because the index might not have existed

        // now we create the index and give it a precise mapping, just like for production
        result = elasticsearch.execInContainer(
            "curl", "https://localhost:9200/books", "-u", "elastic:changeme",
            "--cacert", "/usr/share/elasticsearch/config/certs/http_ca.crt",
            "-X", "PUT",
            "-H", "Content-Type: application/json",
            "-d", """
                {
                  "mappings": {
                    "properties": {
                      "title": { "type": "text" },
                      "description": { "type": "text" },
                      "author": { "type": "text" },
                      "year": { "type": "short" },
                      "publisher": { "type": "text" },
                      "ratings": { "type": "half_float" }
                    }
                  }
                }
                """
        );
        assert result.getExitCode() == 0;

        // now we need to run bulk import of the data we copied to the container
        result = elasticsearch.execInContainer(
            "curl", "https://localhost:9200/_bulk?refresh=true", "-u", "elastic:changeme",
            "--cacert", "/usr/share/elasticsearch/config/certs/http_ca.crt",
            "-X", "POST",
            "-H", "Content-Type: application/x-ndjson",
            "--data-binary", "@/tmp/books.ndjson"
        );
        assert result.getExitCode() == 0;

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

    @Test
    void shouldFetchTheNumberOfBooksPublishedInGivenYear() {
        var systemUnderTest = new BookSearcher(client);
        int books = systemUnderTest.numberOfBooksPublishedInYear(1776);
        Assertions.assertEquals(2, books, "there were 2 books published in 1776 in the dataset");
        // or maybe just one? ;-)
    }
}
