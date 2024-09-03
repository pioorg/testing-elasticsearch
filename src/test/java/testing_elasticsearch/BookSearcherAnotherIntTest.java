package testing_elasticsearch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BookSearcherAnotherIntTest extends CommonTestBase {

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
