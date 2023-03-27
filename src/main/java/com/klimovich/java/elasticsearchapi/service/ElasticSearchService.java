package com.klimovich.java.elasticsearchapi.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.LatLonGeoLocation;
import co.elastic.clients.elasticsearch._types.StoredScriptId;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import com.klimovich.java.elasticsearchapi.entity.Book;
import com.klimovich.java.elasticsearchapi.extractor.BookExtractor;
import com.klimovich.java.elasticsearchapi.searchType.SearchType;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ElasticSearchService {
    private static final String INDEX_NAME = "books";
    private static final String PERCOLATE_INDEX_NAME = "percolate_index";
    private static final String SCRIPT_PARAM_NAME = "ratingParam";

    @Autowired
    private BookExtractor bookExtractor;
    @Autowired
    private ElasticsearchClient client;

    public String updateCsvData(String fileName) {
        return bookExtractor.extractBooksByFileName(fileName).stream()
                .map(this::insertOneBook)
                .reduce((id1, id2) -> id1 + "\n" + id2)
                .orElse("Data is not inserted");
    }

    @SneakyThrows
    private String insertOneBook(Book book) {
        return client.index(i -> i
                        .index(INDEX_NAME)
                        .id(book.getId())
                        .document(book))
                .id();
    }

    @SneakyThrows
    public List<Book> searchBooks(SearchType searchType, String... data) {
        Function<Query.Builder, ObjectBuilder<Query>> buildQuery = switch (searchType) {
            case GEO_QUERY -> buildGeoQuery(data[0], data[1], data[2]);
            case FUZZY_QUERY -> buildFuzzyQuery(data[0], data[1], data[2]);
            case MATCH_QUERY -> buildMatchQuery(data[0], data[1]);
            case MATCH_QUERY_BOOLEAN -> buildMatchBooleanQuery(data[0], data[1], data[2]);
            case MATCH_PHRASE_QUERY -> buildMatchPhraseQuery(data[0], data[1]);
            case MORE_LIKE_THIS_QUERY -> buildMoreLikeThisQuery(data[0], data[1], data[2], data[3], data[4]);
            case PERCOLATE_QUERY -> buildPercolateQuery(data[0], data[1], data[2]);
            case SCRIPT_QUERY -> buildScriptQuery(data[0], data[1]);
        };
        SearchResponse<Book> searchResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(buildQuery),
                Book.class);

        return searchResponse.hits()
                .hits()
                .stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildGeoQuery(String distanceAsString, String lat, String lon) {
        return builder -> builder
                .geoDistance(distance -> distance
                        .distance(distanceAsString)
                        .field("bookLocation")
                        .location(GeoLocation.of(e ->
                                e.latlon(LatLonGeoLocation.of(h ->
                                        h.lat(Double.parseDouble(lat))
                                                .lon(Double.parseDouble(lon)))))));
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildFuzzyQuery(String valueQuery, String field, String level) {
        return builder -> builder
                .fuzzy(fuzzyQuery -> fuzzyQuery.field(field)
                        .fuzziness(level)
                        .value(valueQuery));
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMatchQuery(String queryAsString, String field) {
        return builder -> builder
                .match(query -> query
                        .field(field)
                        .query(queryAsString));
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMatchBooleanQuery(String queryAsString, String mandatoryField, String secondaryField) {
        Query queryByMandatoryField = MatchQuery.of(match -> match.field(mandatoryField).query(queryAsString))._toQuery();
        Query queryBySecondaryField = MatchQuery.of(match -> match.field(secondaryField).query(queryAsString))._toQuery();

        return builder -> builder
                .bool(query -> query
                        .must(queryByMandatoryField)
                        .should(queryBySecondaryField));
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMatchPhraseQuery(String queryAsString, String field) {
        return builder -> builder
                .matchPhrase(phrase -> phrase
                        .field(field)
                        .query(queryAsString));
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMoreLikeThisQuery(String queryAsString,
                                                                                 String firstField,
                                                                                 String secondField,
                                                                                 String minTermFrequency,
                                                                                 String minDocFrequency) {
        return builder -> builder
                .moreLikeThis(query -> query
                        .like(text -> text.text(queryAsString))
                        .fields(List.of(firstField, secondField))
                        .minTermFreq(Integer.parseInt(minDocFrequency))
                        .minDocFreq(Integer.parseInt(minDocFrequency)));

    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildPercolateQuery(String rating, String title, String genre) {
        return builder -> builder
                .percolate(query -> query
                        .index(PERCOLATE_INDEX_NAME)
                        .document(JsonData.of(Book.builder()
                                .rating(Integer.parseInt(rating))
                                .title(title)
                                .genre(genre)
                                .build())));
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildScriptQuery(String scriptId, String ratingAsString) {
        return builder -> builder
                .script(script -> script
                        .script(scriptBuilder -> scriptBuilder
                                .stored(StoredScriptId.of(scriptIdBuilder -> scriptIdBuilder
                                        .id(scriptId)
                                        .params(Map.of(SCRIPT_PARAM_NAME, JsonData.of(ratingAsString)))))));
    }
}










































