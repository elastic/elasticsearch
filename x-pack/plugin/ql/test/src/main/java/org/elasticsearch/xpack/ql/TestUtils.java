/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomZone;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.junit.Assert.assertEquals;

public final class TestUtils {

    public static final ZoneId UTC = ZoneId.of("Z");

    public static final Configuration TEST_CFG = new Configuration(UTC, null, null);

    private TestUtils() {}

    public static Configuration randomConfiguration() {
        return new Configuration(randomZone(), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static Configuration randomConfiguration(ZoneId zoneId) {
        return new Configuration(zoneId, randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static Literal of(Object value) {
        return of(Source.EMPTY, value);
    }

    /**
     * Utility method for creating 'in-line' Literals (out of values instead of expressions).
     */
    public static Literal of(Source source, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        return new Literal(source, value, DataTypes.fromJava(value));
    }

    public static Equals equalsOf(Expression left, Expression right) {
        return new Equals(EMPTY, left, right, randomZone());
    }

    public static NotEquals notEqualsOf(Expression left, Expression right) {
        return new NotEquals(EMPTY, left, right, randomZone());
    }

    public static NullEquals nullEqualsOf(Expression left, Expression right) {
        return new NullEquals(EMPTY, left, right, randomZone());
    }

    public static LessThan lessThanOf(Expression left, Expression right) {
        return new LessThan(EMPTY, left, right, randomZone());
    }

    public static LessThanOrEqual lessThanOrEqualOf(Expression left, Expression right) {
        return new LessThanOrEqual(EMPTY, left, right, randomZone());
    }

    public static GreaterThan greaterThanOf(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right, randomZone());
    }

    public static GreaterThanOrEqual greaterThanOrEqualOf(Expression left, Expression right) {
        return new GreaterThanOrEqual(EMPTY, left, right, randomZone());
    }

    public static Range rangeOf(Expression value, Expression lower, boolean includeLower, Expression upper, boolean includeUpper) {
        return new Range(EMPTY, value, lower, includeLower, upper, includeUpper, randomZone());
    }

    public static FieldAttribute fieldAttribute() {
        return fieldAttribute(randomAlphaOfLength(10), randomFrom(DataTypes.types()));
    }

    public static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(EMPTY, name, new EsField(name, type, emptyMap(), randomBoolean()));
    }

    public static EsRelation relation() {
        return new EsRelation(EMPTY, new EsIndex(randomAlphaOfLength(8), emptyMap()), randomBoolean());
    }

    //
    // Common methods / assertions
    //

    public static void assertNoSearchContexts(RestClient client) throws IOException {
        Map<String, Object> stats = searchStats(client);
        @SuppressWarnings("unchecked")
        Map<String, Object> indicesStats = (Map<String, Object>) stats.get("indices");
        for (String index : indicesStats.keySet()) {
            if (index.startsWith(".") == false) { // We are not interested in internal indices
                assertEquals(index + " should have no search contexts", 0, getOpenContexts(stats, index));
            }
        }
    }

    public static int getNumberOfSearchContexts(RestClient client, String index) throws IOException {
        return getOpenContexts(searchStats(client), index);
    }

    private static Map<String, Object> searchStats(RestClient client) throws IOException {
        Response response = client.performRequest(new Request("GET", "/_stats/search"));
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    @SuppressWarnings("unchecked")
    private static int getOpenContexts(Map<String, Object> stats, String index) {
        stats = (Map<String, Object>) stats.get("indices");
        stats = (Map<String, Object>) stats.get(index);
        stats = (Map<String, Object>) stats.get("total");
        stats = (Map<String, Object>) stats.get("search");
        return (Integer) stats.get("open_contexts");
    }

    //
    // Classpath
    //
    /**
     * Returns the classpath resources matching a simple pattern ("*.csv").
     * It supports folders separated by "/" (e.g. "/some/folder/*.txt").
     *
     * Currently able to resolve resources inside the classpath either from:
     * folders in the file-system (typically IDEs) or
     * inside jars (gradle).
     */
    @SuppressForbidden(reason = "classpath discovery")
    public static List<URL> classpathResources(String pattern) throws IOException {
        while (pattern.startsWith("/")) {
            pattern = pattern.substring(1);
        }

        Tuple<String, String> split = pathAndName(pattern);

        // the root folder searched inside the classpath - default is the root classpath
        // default file match
        final String root = split.v1();
        final String filePattern = split.v2();

        String[] resources = System.getProperty("java.class.path").split(System.getProperty("path.separator"));

        List<URL> matches = new ArrayList<>();

        for (String resource : resources) {
            Path path = PathUtils.get(resource);

            // check whether we're dealing with a jar
            // Java 7 java.nio.fileFileSystem can be used on top of ZIPs/JARs but consumes more memory
            // hence the use of the JAR API
            if (path.toString().endsWith(".jar")) {
                try (JarInputStream jar = jarInputStream(path.toUri().toURL())) {
                    ZipEntry entry = null;
                    while ((entry = jar.getNextEntry()) != null) {
                        String name = entry.getName();
                        Tuple<String, String> entrySplit = pathAndName(name);
                        if (root.equals(entrySplit.v1()) && Regex.simpleMatch(filePattern, entrySplit.v2())) {
                            matches.add(new URL("jar:" + path.toUri() + "!/" + name));
                        }
                    }
                }
            }
            // normal file access
            else if (Files.isDirectory(path)) {
                Files.walkFileTree(path, EnumSet.allOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (Regex.simpleMatch(filePattern, file.toString())) {
                            matches.add(file.toUri().toURL());
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }
        return matches;
    }

    @SuppressForbidden(reason = "need to open stream")
    public static InputStream inputStream(URL resource) throws IOException {
        URLConnection con = resource.openConnection();
        // do not to cache files (to avoid keeping file handles around)
        con.setUseCaches(false);
        return con.getInputStream();
    }

    @SuppressForbidden(reason = "need to open jar")
    public static JarInputStream jarInputStream(URL resource) throws IOException {
        return new JarInputStream(inputStream(resource));
    }

    public static BufferedReader reader(URL resource) throws IOException {
        return new BufferedReader(new InputStreamReader(inputStream(resource), StandardCharsets.UTF_8));
    }

    public static Tuple<String, String> pathAndName(String string) {
        String folder = StringUtils.EMPTY;
        String file = string;
        int lastIndexOf = string.lastIndexOf("/");
        if (lastIndexOf > 0) {
            folder = string.substring(0, lastIndexOf - 1);
            if (lastIndexOf + 1 < string.length()) {
                file = string.substring(lastIndexOf + 1);
            }
        }
        return new Tuple<>(folder, file);
    }
}
