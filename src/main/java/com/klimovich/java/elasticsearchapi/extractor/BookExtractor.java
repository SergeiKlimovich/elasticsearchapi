package com.klimovich.java.elasticsearchapi.extractor;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.klimovich.java.elasticsearchapi.entity.Book;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

@Component
public class BookExtractor {

    public static final String FILENAME = "C:\\MyCoding\\Java\\elasticsearchapi\\elasticsearchapi\\src\\main\\resources\\%s.csv";

    @SneakyThrows
    public List<Book> extractBooksByFileName(String fileName) {
        List<Book> books = new ArrayList<>();
        FileInputStream fileInputStream = new FileInputStream(String.format(FILENAME, fileName));
        CsvMapper csvMapper = new CsvMapper();
        ObjectReader objectReader = csvMapper.reader(Book.class);
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();
        MappingIterator<Book> objectMappingIterator = objectReader.with(csvSchema).readValues(fileInputStream);

        while (objectMappingIterator.hasNext()) {
            Book book = objectMappingIterator.next();
            books.add(book);
        }
        fileInputStream.close();
        return books;
    }
}
