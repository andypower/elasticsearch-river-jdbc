/*
 * Copyright 2014 Nazzareno Sileno - CNR IMAA geoSDI Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.river.jdbc.strategy;

import java.util.List;
import java.util.Map;
import java.util.Random;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class LocationFinder {

    private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

    private final Client client;
    private String countryGeonameID;
    private Map<String, Object> jsonLocation = Maps.<String, Object>newHashMap();

    public LocationFinder(Client client) {
        this.client = client;
    }

    /**
     * Analizza i valori di location: nazione, citta e si ferma quando trova il
     * campo successivo id_area_tematica inserendo la location maggiormente
     * indicativa trovata
     *
     * @param columnLabel
     * @param value
     * @param values
     */
    public void analyzeLocation(String columnLabel, Object value, List<Object> values) {
        logger.debug("Column Label: {}", columnLabel);
        switch (columnLabel) {
            case "nazione":
                this.jsonLocation = this.findCountry(value);
                logger.debug("Trovata nazione: {}", this.jsonLocation);
                break;
            case "citta":
                Map<String, Object> cityAddress = this.findCity(value);
                if (cityAddress.get("location") != null) {
                    this.jsonLocation = cityAddress;
                    logger.debug("Trovata citta: {}", this.jsonLocation);
                }
                ;
                break;
            case "id_area_tematica":
                //Adds the country or the city found
                values.add(Lists.newArrayList(jsonLocation));
                logger.debug("Aggiunta location: {}", this.jsonLocation);
                logger.debug("Lista di valori: {}", values);
                break;
        }
    }

    private Map<String, Object> findCity(Object value) {
        Map<String, Object> json = Maps.<String, Object>newHashMap();
        json.put("address", value);
        if (value != null && !((String)value).isEmpty()) {
            SearchHit[] docs = this.executeCityGeoNameQuery(value);
            if (docs.length != 0) {
                logger.debug("!!! Found city={}", docs[0].getSourceAsString());
                double longitude = Double.parseDouble(docs[0].getSource().get("longitude").toString());
                double latitude = Double.parseDouble(docs[0].getSource().get("latitude").toString());
                json.put("location", this.shiftLonLatPoint(latitude, longitude));
            }
        }
        return json;
    }

    private Map<String, Object> findCountry(Object value) {
        Map<String, Object> json = Maps.<String, Object>newHashMap();
        json.put("address", value);
        if (value != null && !((String)value).isEmpty()) {
            SearchHit[] docs = this.executeCountryGeoNameQuery(value);
            if (docs.length != 0) {
                logger.debug("!!! Found country={}", docs[0].getSourceAsString());
                double longitude = Double.parseDouble(docs[0].getSource().get("longitude").toString());
                double latitude = Double.parseDouble(docs[0].getSource().get("latitude").toString());
                this.countryGeonameID = docs[0].getSource().get("geonameid").toString();
                json.put("location", this.shiftLonLatPoint(latitude, longitude));
            }
        }
        return json;
    }

    private SearchHit[] executeCityGeoNameQuery(Object value) {
        FilterBuilder typeCityFilter = null;
        if (Strings.hasText(countryGeonameID)) {
            typeCityFilter = FilterBuilders.andFilter(
                    FilterBuilders.termFilter("parents.geonameid", countryGeonameID),
                    FilterBuilders.termFilter("type", "city"));
        } else {
            typeCityFilter = FilterBuilders.termFilter("type", "city");
        }

        SearchResponse response = client.prepareSearch("geonames").setSearchType(
                SearchType.DFS_QUERY_THEN_FETCH).setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.matchQuery("name", value), typeCityFilter))
                .setFrom(0).setSize(1).setExplain(true).execute().actionGet();

        SearchHit[] docs = response.getHits().getHits();
        if (docs.length == 0) {
            response = client.prepareSearch("geonames").setSearchType(
                    SearchType.DFS_QUERY_THEN_FETCH).setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.fuzzyQuery("name", value),
                                    typeCityFilter))
                    .setFrom(0).setSize(1).setExplain(true).execute().actionGet();
            docs = response.getHits().getHits();
        }
        if (docs.length == 0) {
            response = client.prepareSearch("geonames").setSearchType(
                    SearchType.DFS_QUERY_THEN_FETCH).setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.moreLikeThisFieldQuery("name").
                                    likeText((String) value), typeCityFilter))
                    .setFrom(0).setSize(1).setExplain(true).execute().actionGet();
            docs = response.getHits().getHits();
        }
        return docs;
    }

    private SearchHit[] executeCountryGeoNameQuery(Object value) {
        SearchResponse response = client.prepareSearch("geonames").setSearchType(
                SearchType.DFS_QUERY_THEN_FETCH).setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.matchQuery("name", value),
                                //                                        termQuery(
                                //                                        commonTerms(
                                //                                        matchQuery(
                                //                                        fieldQuery("name", value),
                                FilterBuilders.termFilter("type", "country")))
                .setFrom(0).setSize(1).setExplain(true).execute().actionGet();

        SearchHit[] docs = response.getHits().getHits();
        if (docs.length == 0) {
            response = client.prepareSearch("geonames").setSearchType(
                    SearchType.DFS_QUERY_THEN_FETCH).setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.fuzzyQuery("name", value),
                                    FilterBuilders.termFilter("type", "country")))
                    .setFrom(0).setSize(1).setExplain(true).execute().actionGet();
            docs = response.getHits().getHits();
        }
        if (docs.length == 0) {
            response = client.prepareSearch("geonames").setSearchType(
                    SearchType.DFS_QUERY_THEN_FETCH).setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.moreLikeThisFieldQuery("name").
                                    likeText((String) value),
                                    FilterBuilders.termFilter("type", "country")))
                    .setFrom(0).setSize(1).setExplain(true).execute().actionGet();
            docs = response.getHits().getHits();
        }
        return docs;
    }

    /**
     *
     * @param lon
     * @param lat
     * @return Shift the lon lat using a random distance between 1Km
     */
    private GeoPoint shiftLonLatPoint(double lat, double lon) {
        //Earth’s radius, sphere
        final double earthRadius = 6378137;
        //offsets in meters
        Random rand = new Random();
        double dn = (rand.nextDouble() - 0.5d) * 2000; //Returns a number between [-1000, 1000]
        double de = (rand.nextDouble() - 0.5d) * 2000;
//        final double dn = rand.nextInt(1001);
//        final double de = rand.nextInt(1001);
//        lon += rand.nextDouble() * 0.001;
//        lat += rand.nextDouble() * 0.001;
        //Coordinate offsets in radians             
        final double dLat = dn / earthRadius;
        final double dLon = de / (earthRadius * Math.cos(Math.PI * lat / 180));
        //OffsetPosition, decimal degrees
        final double latO = lat + dLat * 180 / Math.PI;
        final double lonO = lon + dLon * 180 / Math.PI;
        return new GeoPoint(latO, lonO);
//        return new GeoPoint(lat, lon);
    }

}
