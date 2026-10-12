/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Every boundary where a query's config map crosses the wire must drop the coordinator-only dataset version
 * first, because a data node builds its storage provider from that map and {@code StorageProviderCache} keys on
 * the whole of it — an older node, whose {@code FRAMEWORK_KEYS} cannot know the key, fragments its client pool
 * per dataset definition and throws at the pool's ceiling.
 * <p>
 * A census rather than three assertions, because fixing the boundaries one at a time already failed twice here:
 * {@code ExternalSourceExec} was fixed first, then {@code ExternalRelation} was found to be the route a
 * {@code FROM} over a dataset actually takes, then {@code FileSplit} was found to carry it to every data node as
 * well. A fourth would have been just as quiet. This fails the build on a new one.
 */
public class ConfigWireBoundaryCensusTests extends ESTestCase {

    private static final Pattern GENERIC_WRITE = Pattern.compile("out\\.writeGeneric(?:Value|Map)\\(");

    public void testEveryConfigWireBoundaryDropsTheCoordinatorOnlyKey() throws IOException {
        Path root = esqlMainSources();
        List<String> guarded = new ArrayList<>();
        List<String> unguarded = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(".java") == false) {
                    return FileVisitResult.CONTINUE;
                }
                String source = Files.readString(file, StandardCharsets.UTF_8);
                Matcher write = GENERIC_WRITE.matcher(source);
                while (write.find()) {
                    // The enclosing writeTo, which is where a local holding the stripped map would be assigned -
                    // one of the three boundaries wraps indirectly, so matching only the argument misses it.
                    int signature = source.lastIndexOf("writeTo(StreamOutput", write.start());
                    if (signature < 0) {
                        continue; // not inside a writeTo, so not a wire boundary this census governs
                    }
                    String body = source.substring(signature, Math.min(source.length(), write.end() + 200));
                    if (body.toLowerCase(Locale.ROOT).contains("config") == false) {
                        continue; // this write carries something else
                    }
                    String site = root.relativize(file).toString();
                    (body.contains("wireConfig") ? guarded : unguarded).add(site);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        assertFalse("the census found no config wire boundaries, so it is checking nothing", guarded.isEmpty());
        assertEquals(
            "a writeTo carrying a query config map does not route it through ExternalSourceResolver.wireConfig, so "
                + "the coordinator-only dataset version reaches a data node. Wrap it, or say here why that boundary "
                + "is exempt.",
            List.of(),
            unguarded.stream().distinct().toList()
        );
    }

    private static Path esqlMainSources() {
        String probe = "esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplit.java";
        Path cur = PathUtils.get("").toAbsolutePath();
        for (int i = 0; i < 12 && cur != null; i++, cur = cur.getParent()) {
            if (Files.exists(cur.resolve(probe))) {
                return cur.resolve("esql/src/main/java/org/elasticsearch/xpack/esql");
            }
            if (Files.exists(cur.resolve("x-pack/plugin").resolve(probe))) {
                return cur.resolve("x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql");
            }
        }
        throw new AssertionError("cannot locate the esql main sources from " + PathUtils.get("").toAbsolutePath());
    }
}
