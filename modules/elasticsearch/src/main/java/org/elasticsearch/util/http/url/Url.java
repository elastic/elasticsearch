/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.elasticsearch.util.http.url;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Url implements Cloneable, Serializable {
    private static final long serialVersionUID = 2187287725357847401L;

    public enum Protocol {
        HTTP(80),
        HTTPS(443);

        private int port;

        Protocol(int port) {
            this.port = port;
        }

        public int getPort() {
            return port;
        }
    }

    private String scheme;
    private String host;
    private int port;
    private String path;

    private Map<String, List<String>> params = new LinkedHashMap<String, List<String>>();

    /**
     * Constructs a Url object from the given url string. The string is expected to be compliant with the URL RFC
     * and all relevant parts need to be properly url-encoded
     *
     * @param value the string
     * @return a url object
     * @throws MalformedURLException
     */
    public static Url valueOf(String value)
            throws MalformedURLException {
        URL url = new URL(value);
        try {
            return new Url(url.getProtocol(),
                    url.getHost(),
                    // url.getPort() return -1 rather than default for the protocol/scheme if no port is specified.
                    url.getPort() > 0 ? url.getPort() : Protocol.valueOf(url.getProtocol().toUpperCase()).getPort(),
                    URLDecoder.decode(url.getPath(), "UTF-8"),
                    url.getQuery());
        }
        catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * Constructs a Url object from the given url string parts. The parts are expected to be compliant with the URL RFC
     * and need to be properly url-encoded
     */

    public static Url valueOf(String scheme, String host, String uri) {
        try {
            return Url.valueOf(scheme + "://" + host + uri);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Resolves 'path' with respect to a given base url
     */
    public static Url valueOf(Url base, String uri)
            throws MalformedURLException {
        final Url result;
        if (!uri.matches("[\\p{Alpha}][\\p{Alnum}-.+_]+://.+")) { // relative url?
            result = base.clone();
            if (uri.startsWith("/")) { // relative to root?
                result.setUri(uri);
            } else { // relative to current dir
                String path = base.getPath();

                int index = path.lastIndexOf('/');
                if (index != -1) {
                    path = path.substring(0, index + 1) + uri;
                } else {
                    path = "/" + uri;
                }

                result.setUri(path);
            }
        } else { // uri is absolute
            result = Url.valueOf(uri);
        }

        return result;
    }

    private Url(String scheme, String host, int port, String path, Map<String, List<String>> params) {
        setScheme(scheme);
        setHost(host);
        this.port = port;
        this.path = path;
        this.params = new LinkedHashMap<String, List<String>>(params);
    }


    public Url(String scheme, String host, int port, String path, String query) {
        setScheme(scheme);
        setHost(host);
        this.port = port;
        this.path = path;

        setQueryString(query);
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String s) {
        if (Protocol.valueOf(s.toUpperCase()) == null) {
            throw new IllegalArgumentException("Illegal scheme used [" + s + "]");
        }
        this.scheme = s;
    }

    public Protocol getProtocol() {
        return Protocol.valueOf(scheme.toUpperCase());
    }

    /**
     * Gets the path part of this url. The value is url-decoded.
     *
     * @return he path part of this url. The value is url-decoded.
     */
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host.toLowerCase();
    }


    public void setBaseUrl(String url) {
        Pattern pattern = Pattern.compile("([^:]+)://([^:]+)(:([0-9]+))?");

        Matcher matcher = pattern.matcher(url);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid url: " + url);
        }

        setScheme(matcher.group(1));
        setHost(matcher.group(2));

        String port = matcher.group(4);
        if (port != null) {
            this.port = Integer.valueOf(port);
        } else {
            Protocol protocol = getProtocol();
            this.port = protocol.getPort();
        }
    }

    public String getBaseUrl() {
        StringBuilder builder = new StringBuilder();

        builder.append(scheme);
        builder.append("://");
        builder.append(getLocation());

        return builder.toString();
    }

    public String getLocation() {
        StringBuilder builder = new StringBuilder(host);
        int defaultPort;
        try {
            Protocol protocol = getProtocol();
            defaultPort = protocol.getPort();
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to fetch protocol", e);
        }

        if (port != defaultPort) {
            builder.append(':');
            builder.append(port);
        }

        return builder.toString();
    }

    public void setUri(String uri) {
        int index = uri.indexOf('?');

        if (index != -1) {
            path = uri.substring(0, index);
            String query = uri.substring(index + 1);
            setQueryString(query);
        } else {
            path = uri;
            params.clear();
        }
    }

    public String getUri() {
        StringBuilder builder = new StringBuilder();

        builder.append(path);
        if (!params.isEmpty()) {
            builder.append('?');
            builder.append(getQueryString());
        }

        return builder.toString();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getBaseUrl());
        builder.append(getUri());

        return builder.toString();
    }

    /**
     * Return the URL string, but without any parameters
     *
     * @return a string
     */
    public String toStringWithoutParams() {
        return new Url(scheme, host, port, path, "").toString();
    }


    private void parseParameters(String query) {
        params.clear();
        if (query != null) {
            StringTokenizer tokenizer = new StringTokenizer(query, "&");

            while (tokenizer.hasMoreElements()) {
                String token = tokenizer.nextToken();
                String[] param = token.split("=", 2);
                if (param.length > 0) {
                    String name = param[0];
                    String value = param.length > 1
                            ? urlDecode(param[1])
                            : null; // null case distinguishes between ?name= and ?name

                    if (name.length() > 0) {
                        addParameter(name, value);
                    }
                }
            }
        }
    }

    private String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            // this should not be possible
            throw new IllegalStateException("Should never happen: UTF-8 encoding is unsupported.", e);
        }
    }

    public void setParameter(String name, String value) {
        removeParameter(name);
        addParameter(name, value);
    }

    /**
     * reset the parameters list.
     */
    public void clearParameters() {
        params.clear();
    }

    public void setParameter(String name, Object value) {
        setParameter(name, value.toString());
    }

    public void addParameter(String name, String value) {
        List<String> list = params.get(name);
        if (list == null) {
            list = new ArrayList<String>();
            params.put(name, list);
        }
        list.add(value);
        //params.add(new Pair<String, String>(name, value));
    }

    public String getParameter(String name) {
        String result = null;
        List<String> list = params.get(name);
        if (list != null && list.size() > 0) {
            result = list.get(0);
        }
        return result;
    }

    public List<String> getParameters(String name) {
        List<String> result = new ArrayList<String>();

        List<String> list = params.get(name);
        if (list != null && list.size() > 0) {
            result.addAll(list);
        }

        return result;
    }

    public Map<String, List<String>> getParameters() {
        return params;
    }


    public void removeParameter(String name) {
        params.remove(name);
    }

    public void setQueryString(String query) {
        // TODO: don't parse until addParameter/setParameter/removeParameter is called
        parseParameters(query);
    }

    public String getQueryString() {
        // TODO: cache query string
        String result = null;

        if (!params.isEmpty()) {
            StringBuilder builder = new StringBuilder();

            for (Iterator<Map.Entry<String, List<String>>> i = params.entrySet().iterator(); i.hasNext();) {
                Map.Entry<String, List<String>> param = i.next();
                String name = param.getKey();
                for (Iterator<String> j = param.getValue().iterator(); j.hasNext();) {
                    String value = j.next();
                    builder.append(name);
                    if (value != null) {
                        builder.append('=');
                        try {
                            builder.append(URLEncoder.encode(value, "UTF-8"));
                        }
                        catch (UnsupportedEncodingException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                    if (j.hasNext()) {
                        builder.append('&');
                    }
                }
                if (i.hasNext()) {
                    builder.append('&');
                }
            }

            result = builder.toString();
        }

        return result;
    }


    public Url clone() {
        return new Url(scheme, host, port, path, params);
    }

    public int hashCode() {
        return toString().hashCode();
    }

    public boolean equals(Object obj) {
        // TODO: we should compare piece by piece. Argument ordering shouldn't affect equality
        return obj instanceof Url && toString().equals(obj.toString());
    }

    /**
     * Is the given Url string in a valid format?
     *
     * @param url
     * @return true if valid
     */
    public static boolean isValidUrl(String url) {
        try {
            Url.valueOf(url);
            return true;
        }
        catch (MalformedURLException e) {
            return false;
        }
    }
}
