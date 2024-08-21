package testing_elasticsearch;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;

public class CSV2JSONConverter {

    public static void convert(String url, Writer output, Class<?> klass, String indexName) throws IOException, InterruptedException {

        LocalTime started = LocalTime.now();
        System.out.println("Starting up with data conversion " + started);

        // this is to tell what's the structure of our CSV data
        CsvMapper csvMapper = new CsvMapper();
        CsvSchema schema = csvMapper
            .typedSchemaFor(klass)
            .withHeader()
            .withColumnSeparator(';')
            .withSkipFirstDataRow(true);

        try (
            HttpClient httpClient = HttpClient.newHttpClient();
        ) {
            var bufferedWriter = new BufferedWriter(output);

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            // instead of fetching the whole file locally first, we're going to stream it
            InputStream csvContentStream = response.body();

            // from now on we stream the data and add it
            MappingIterator<?> it = csvMapper
                .readerFor(klass)
                .with(schema)
                .readValues(new InputStreamReader(csvContentStream));
            ObjectMapper objectMapper = new ObjectMapper();

            String meta = switch (indexName) {
                case String s when !s.isBlank() -> """
                    {"index":{"_index":"%s"}}
                    """.formatted(indexName);
                case null, default -> """
                    {"index":{}}
                    """;
            };
            boolean hasNextValue = true;

            while (hasNextValue) {
                try {
                    var nextValue = it.nextValue();
                    bufferedWriter.write(meta);
                    bufferedWriter.write(objectMapper.writeValueAsString(nextValue));
                    bufferedWriter.newLine();
                    hasNextValue = it.hasNextValue();

                } catch (JsonParseException | InvalidFormatException e) {
                    // ignore malformed data
                }
            }
            // bulk import has to end with an empty line
            bufferedWriter.newLine();
            bufferedWriter.flush();
        }

        LocalTime finished = LocalTime.now();
        System.out.println("Finished data conversion " + finished + ", so it took " + Duration.between(started, finished).getSeconds() + " seconds.");
    }
}