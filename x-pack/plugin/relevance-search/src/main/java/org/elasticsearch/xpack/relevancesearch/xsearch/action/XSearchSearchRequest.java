/// *
// * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// * or more contributor license agreements. Licensed under the Elastic License
// * 2.0; you may not use this file except in compliance with the Elastic License
// * 2.0.
// */
//
// package org.elasticsearch.xpack.relevancesearch.xsearch.action;
//
// import org.elasticsearch.action.search.SearchRequest;
// import org.elasticsearch.common.io.stream.StreamInput;
//
// import java.io.IOException;
//
// public class XSearchSearchRequest extends SearchRequest {
//
// public String query;
//
// XSearchSearchRequest(StreamInput in) throws IOException {
// super(in);
// }
//
// public XSearchSearchRequest(String engine, String query) {
// super(engine);
// this.query = query;
// }
//
// public void setQuery(String query) {
// this.query = query;
// }
// }
