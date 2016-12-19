/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    private static final List<String> EMPTY_TYPES = Collections.emptyList();
    private static final List<String> TWO_TYPES = Arrays.asList("type_1", "type_2");

    public void testBuildIndexSettingsUrl() {
        String url = ElasticsearchUrlBuilder.create(SINGLE_INDEX, TWO_TYPES).buildIndexSettingsUrl("foo");
        assertEquals("http://localhost:9200/foo/_settings", url);
    }

    public void testBuildInitScrollUrl_GivenMultipleIndicesAndTypes() {
        String url = ElasticsearchUrlBuilder.create(TWO_INDEXES, TWO_TYPES).buildInitScrollUrl(5000);
        assertEquals("http://localhost:9200/index_1,index_2/type_1,type_2/_search?scroll=60m&size=5000", url);
    }

    public void testBuildContinueScrollUrl() {
        String url = ElasticsearchUrlBuilder.create(SINGLE_INDEX, TWO_TYPES).buildContinueScrollUrl();
        assertEquals("http://localhost:9200/_search/scroll?scroll=60m", url);
    }

    public void testBuildClearScrollUrl() {
        String url = ElasticsearchUrlBuilder.create(SINGLE_INDEX, TWO_TYPES).buildClearScrollUrl();
        assertEquals("http://localhost:9200/_search/scroll", url);
    }

    public void testBuildSearchSizeOneUrl_GivenMultipleIndicesAndTypes() {
        String url = ElasticsearchUrlBuilder.create(TWO_INDEXES, TWO_TYPES).buildSearchSizeOneUrl();
        assertEquals("http://localhost:9200/index_1,index_2/type_1,type_2/_search?size=1", url);
    }

    public void testBuildSearchSizeOneUrl_GivenMultipleIndicesAndEmptyTypes() {
        String url = ElasticsearchUrlBuilder.create(TWO_INDEXES, EMPTY_TYPES).buildSearchSizeOneUrl();
        assertEquals("http://localhost:9200/index_1,index_2/_search?size=1", url);
    }

    public void testGetBaseUrl_GivenNoEndingSlash() {
        String url = ElasticsearchUrlBuilder.create(SINGLE_INDEX, TWO_TYPES).getBaseUrl();
        assertEquals("http://localhost:9200/", url);
    }

    public void testGetBaseUrl_GivenEndingSlash() {
        String url = ElasticsearchUrlBuilder.create(SINGLE_INDEX, TWO_TYPES).getBaseUrl();
        assertEquals("http://localhost:9200/", url);
    }
}
