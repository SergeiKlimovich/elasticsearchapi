package com.klimovich.java.elasticsearchapi.controller;

import com.klimovich.java.elasticsearchapi.entity.Book;
import com.klimovich.java.elasticsearchapi.searchType.SearchType;
import com.klimovich.java.elasticsearchapi.service.ElasticSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/books")
public class ElasticSearchController {

    @Autowired
    private ElasticSearchService service;

    @GetMapping
    public String putData(@RequestParam(name = "name") String name) {
        return service.updateCsvData(name);
    }

    @GetMapping("/search/geoQuery")
    public List<Book> geoQuery(@RequestParam(name = "distance") String distance,
                               @RequestParam(name = "lat") String lat,
                               @RequestParam(name = "lon") String lon) {
        return service.searchBooks(SearchType.GEO_QUERY, null, distance, lat, lon);
    }

    @GetMapping("/search/fuzzyQuery")
    public List<Book> fuzzyQuery(@RequestParam(name = "query") String query,
                                 @RequestParam(name = "field") String field,
                                 @RequestParam(name = "level", defaultValue = "auto", required = false) String level) {
        return service.searchBooks(SearchType.FUZZY_QUERY, query, field, level);
    }

    @GetMapping("/search/matchQuery")
    public List<Book> matchQuery(@RequestParam(name = "query") String query,
                                 @RequestParam(name = "field") String field) {
        return service.searchBooks(SearchType.MATCH_QUERY, query, field);
    }

    @GetMapping("/search/matchBooleanQuery")
    public List<Book> matchQueryBoolean(@RequestParam(name = "query") String query,
                                        @RequestParam(name = "mandatoryField") String mandatoryField,
                                        @RequestParam(name = "secondaryField") String secondaryField) {
        return service.searchBooks(SearchType.MATCH_QUERY_BOOLEAN, query, mandatoryField, secondaryField);
    }

    @GetMapping("/search/matchPhraseQuery")
    public List<Book> matchPhraseQuery(@RequestParam(name = "query") String query,
                                       @RequestParam(name = "field") String field) {
        return service.searchBooks(SearchType.MATCH_PHRASE_QUERY, query, field);
    }

    @GetMapping("/search/mltQuery")
    public List<Book> moreLikeThisQuery(@RequestParam(name = "query") String query,
                                        @RequestParam(name = "field1") String field1,
                                        @RequestParam(name = "field2") String field2,
                                        @RequestParam(name = "minTermFreq", defaultValue = "0", required = false) String minTermFreq,
                                        @RequestParam(name = "minDocFreq", defaultValue = "0", required = false) String minDocFreq) {
        return service.searchBooks(SearchType.MORE_LIKE_THIS_QUERY, query, field1, field2, minTermFreq, minDocFreq);
    }

    @GetMapping("/search/percolateQuery")
    public List<Book> percolateQuery(@RequestParam(name = "rating") String rating,
                                     @RequestParam(name = "title") String title,
                                     @RequestParam(name = "genre") String genre) {
        return service.searchBooks(SearchType.PERCOLATE_QUERY, null, rating, title, genre);
    }

    @GetMapping("/search/scriptQuery")
    public List<Book> scriptQuery(@RequestParam(name = "id") String id,
                                  @RequestParam(name = "rating") String rating) {
        return service.searchBooks(SearchType.SCRIPT_QUERY, null, id, rating);
    }
}
