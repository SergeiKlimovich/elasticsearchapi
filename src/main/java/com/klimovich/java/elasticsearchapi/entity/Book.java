package com.klimovich.java.elasticsearchapi.entity;


import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
public class Book {
    private String id;
    private String title;
    private String genre;
    private int rating;
    private String geoLocation;

}
