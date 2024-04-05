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
import co.elastic.clients.elasticsearch.esql.ElasticsearchEsqlClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BookSearcherMockingTest {

    ResultSet mockResultSet;
    ElasticsearchClient esClient;
    ElasticsearchEsqlClient esql;

    @BeforeEach
    void setUpMocks() {
        mockResultSet = mock(ResultSet.class);
        esClient = mock(ElasticsearchClient.class);
        esql = mock(ElasticsearchEsqlClient.class);

    }

    @Test
    void canCreateSearcherWithES_8_14() throws SQLException, IOException{
        // when
        when(esClient.esql()).thenReturn(esql);
        when(esql.query(eq(ResultSetEsqlAdapter.INSTANCE), anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        // here you need to know which inner stuff you need to mock
        // and what will happen if the implementation is changed to "major" and "minor"?
        when(mockResultSet.getInt(1)).thenReturn(8);
        when(mockResultSet.getInt(2)).thenReturn(14);

        // then
        Assertions.assertDoesNotThrow(() -> new BookSearcher(esClient));
    }

    @Test
    void cannotCreateSearcherWithoutES_8_14() throws SQLException, IOException {
        // when
        when(esClient.esql()).thenReturn(esql);
        when(esql.query(eq(ResultSetEsqlAdapter.INSTANCE), anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.getInt(1)).thenReturn(8);
        when(mockResultSet.getInt(2)).thenReturn(15);

        // then
        Assertions.assertThrows(UnsupportedOperationException.class, () -> new BookSearcher(esClient));
    }

    @Test
    void cannotCreateSearcherWithoutESVersion() throws SQLException, IOException {
        // when
        when(esClient.esql()).thenReturn(esql);
        when(esql.query(eq(ResultSetEsqlAdapter.INSTANCE), anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(false);

        // then
        Assertions.assertThrows(RuntimeException.class, () -> new BookSearcher(esClient));
    }

    @Test
    void shouldNotAllowSearchingForMostPublishedAuthorsWithIncorrectDates() throws SQLException, IOException {
        // when
        when(esClient.esql()).thenReturn(esql);
        when(esql.query(eq(ResultSetEsqlAdapter.INSTANCE), anyString())).thenReturn(mockResultSet);

        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.getInt(1)).thenReturn(8);
        when(mockResultSet.getInt(2)).thenReturn(14);

        BookSearcher systemUnderTest = new BookSearcher(esClient);

        // then
        Assertions.assertThrows(
            AssertionError.class,
            () -> systemUnderTest.mostPublishedAuthorsInYears(2012, 2000)
        );

    }
}
