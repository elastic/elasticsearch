/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

/**
 * A basic http response abstraction. Http modules must implement this interface as the server package rest
 * handling needs to set http headers for a response.
 */
public interface HttpResponse {

    void addHeader(String name, String value);

    boolean containsHeader(String name);

}
