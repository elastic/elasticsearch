package org.elasticsearch.river.couchdb.http;

import org.elasticsearch.river.couchdb.run.Defect;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.List;
import java.util.ListIterator;

import static org.elasticsearch.common.collect.Lists.*;

public class UriBuilder {
    private final String baseUrl;
    private final List<QueryParameter> parameters = newArrayList();
    private final List<String> paths = newArrayList();
    private Integer port;

    public UriBuilder(URI baseUrl) {
        this(baseUrl.toString());
    }

    public UriBuilder(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public URI build() {
        try {
            return new URI(urlText());
        }
        catch (URISyntaxException e) {
            throw new Defect("unable to create uri");
        }
    }

    public String urlText() {
        return base() + path() + queryString();
    }

    public URI url() {
        return build();
    }

    private StringBuilder base() {
        StringBuilder base = new StringBuilder(baseUrl);
        if (port != null) {
            base.append(":").append(port);
        }
        return base;
    }

    private String path() {
        StringBuilder path = new StringBuilder();
        if (!paths.isEmpty()) {
            for (ListIterator<String> it = paths.listIterator(); it.hasNext();) {
                if (path.length() == 0) {
                    path.append("/");
                }

                path.append(it.next());

                if (it.hasNext()) {
                    path.append("/");
                }
            }
        }

        return escapePath(path);
    }

    private StringBuilder queryString() {
        StringBuilder queryString = new StringBuilder();
        if (!parameters.isEmpty()) {
            queryString.append("?");
            for (ListIterator<QueryParameter> it = parameters.listIterator(); it.hasNext();) {
                queryString.append(it.next().asUrlPart());
                if (it.hasNext()) {
                    queryString.append("&");
                }
            }
        }
        return queryString;
    }

    public UriBuilder addQueryParameter(String name, String value) {
        parameters.add(new ValuedQueryParameter(name, value));
        return this;
    }

    public UriBuilder addPath(String pathComponent) {
        paths.add(pathComponent.startsWith("/") ? pathComponent.substring(1, pathComponent.length()) : pathComponent);
        return this;
    }

    public UriBuilder addQueryParameter(String name) {
        return addQueryParameter(new UnvaluedQueryParameter(name));
    }

    public UriBuilder addQueryParameter(QueryParameter unvaluedQueryParameter) {
        parameters.add(unvaluedQueryParameter);
        return this;
    }

    public UriBuilder addQueryParameters(String name, Iterable<String> values) {
        for (String value : values) {
            addQueryParameter(name, value);
        }
        return this;
    }

    public UriBuilder port(Integer port) {
        this.port = port;
        return this;
    }

    public interface QueryParameter {
        String asUrlPart();
    }

    private String escapePath(StringBuilder builder) {
        String path = builder.toString();
        try {
            return new URI("dontcare", "", path, "").getRawPath();
        }
        catch (URISyntaxException e) {
            throw new Defect("Cannot escape " + path);
        }
    }

    public static class ValuedQueryParameter implements QueryParameter {
        private final String name;
        private final String value;

        public ValuedQueryParameter(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String asUrlPart() {
            try {
                return name + "=" + URLEncoder.encode(value, "UTF-8");
            }
            catch (UnsupportedEncodingException e) {
                throw new Defect("Cannot encode " + value);
            }
        }
    }

    private class UnvaluedQueryParameter implements QueryParameter {
        private final String name;

        public UnvaluedQueryParameter(String name) {
            this.name = name;
        }

        public String asUrlPart() {
            return name;
        }
    }

}
