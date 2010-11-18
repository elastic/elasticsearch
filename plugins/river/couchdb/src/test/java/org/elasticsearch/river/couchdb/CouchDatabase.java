package org.elasticsearch.river.couchdb;

import org.elasticsearch.river.couchdb.http.Http;
import org.elasticsearch.river.couchdb.http.UriBuilder;

import java.io.IOException;
import java.net.URI;

public class CouchDatabase {
    private final Http http;
    private final URI databaseUri;
    private final String name;

    public CouchDatabase(Http http, String name, URI databaseUri) {
        this.http = http;
        this.name = name;
        this.databaseUri = databaseUri;
    }

    public String name() {
        return name;
    }

    public void createDocument(String id, String jsonContent) throws IOException {

        URI documentUri = new UriBuilder(databaseUri).addPath(id).build();

        Http.HttpResult result = http.put(documentUri, jsonContent, "application/json");

        if ( ! result.ok() ) {
            if ( result.error()) {
                throw new IOException("Problem in couch db " + result);
            }
            throw new CouchCreateFailedException(id, jsonContent, result);
        }
    }

    public static class CouchCreateFailedException extends IOException {
        private final String id;
        private final String jsonContent;
        private final Http.HttpResult result;

        public CouchCreateFailedException(String id, String jsonContent, Http.HttpResult result) {
            super("Cannot create " + id + ", result " + result.status + " " + result.body);
            this.id = id;
            this.jsonContent = jsonContent;
            this.result = result;
        }

        @Override public String toString() {
            return "CouchCreateFailedException{" +
                    "id='" + id + '\'' +
                    ", jsonContent='" + jsonContent + '\'' +
                    ", result=" + result +
                    '}';
        }
    }
}
