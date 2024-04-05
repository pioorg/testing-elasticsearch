/*
 *  Copyright (C) 2024 Piotr Przybył
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package testing_elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.esql.jdbc.ResultSetEsqlAdapter;
import co.elastic.clients.elasticsearch._helpers.esql.objects.ObjectsEsqlAdapter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Piotr Przybył
 */
public class BookSearcher {

    private final ElasticsearchClient esClient;

    public BookSearcher(ElasticsearchClient esClient) {
        this.esClient = esClient;
        if (!isCompatibleWithBackend()) {
            throw new UnsupportedOperationException("This is not compatible with backend");
        }
    }

    private boolean isCompatibleWithBackend() {
        try (ResultSet rs = esClient.esql().query(ResultSetEsqlAdapter.INSTANCE, """
            show info
            | keep version
            | dissect version "%{major}.%{minor}.%{patch}"
            | keep major, minor
            | limit 1""")) {
            if (!rs.next()) {
                throw new RuntimeException("No version found");
            }
            return rs.getInt(1) == 8 && rs.getInt(2) == 14;
//            return rs.getInt("major") == 8 && rs.getInt("minor") == 14;
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int numberOfBooksPublishedInYear(int year) {
        try (ResultSet rs = esClient.esql().query(ResultSetEsqlAdapter.INSTANCE, """
            from books
            | where year == ?
            | stats published = count(*) by year
            | limit 1000""", year)) {

            if (rs.next()) {
                return rs.getInt("published");
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }


    public List<MostPublished> mostPublishedAuthorsInYears(int minYear, int maxYear) {
        assert minYear <= maxYear;
        String query = """
            from books
            | where year >= ? and year <= ?
            | stats first_published = min(year), last_published = max(year), times = count (*) by author
            | eval years_published = last_published - first_published
            | sort years_published desc
            | drop years_published
            | limit 20
            """;

        try {
            Iterable<MostPublished> published = esClient.esql().query(
                ObjectsEsqlAdapter.of(MostPublished.class),
                query,
                minYear,
                maxYear);

            List<MostPublished> mostPublishedAuthors = new ArrayList<>();
            for (MostPublished mostPublished : published) {
                mostPublishedAuthors.add(mostPublished);
            }
            return mostPublishedAuthors;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public record MostPublished(
        String author,
        @JsonProperty("first_published") int firstPublished,
        @JsonProperty("last_published") int lastPublished,
        int times
    ) {
        public MostPublished {
            assert author != null;
            assert firstPublished <= lastPublished;
            assert times > 0;
        }
    }
}
