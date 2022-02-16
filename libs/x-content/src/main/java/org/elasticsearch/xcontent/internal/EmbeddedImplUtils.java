/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmbeddedImplUtils {

    private EmbeddedImplUtils() {}

    private static final String IMPL_PREFIX = "IMPL-JARS/";
    private static final String MANIFEST_FILE = "/LISTING.TXT";

    static Map<String, CodeSource> getProviderPrefixes(ClassLoader parent, String providerName) {
        String providerPrefix = IMPL_PREFIX + providerName;
        URL manifest = parent.getResource(providerPrefix + MANIFEST_FILE);
        if (manifest == null) {
            throw new IllegalStateException("missing x-content provider jars list");
        }
        try (
            InputStream in = manifest.openStream();
            InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr)
        ) {
            List<String> jars = reader.lines().toList();
            Map<String, CodeSource> map = new HashMap<>();
            for (String jar : jars) {
                map.put(providerPrefix + "/" + jar, new CodeSource(new URL(manifest, jar), (CodeSigner[]) null /*signers*/));
            }
            return Collections.unmodifiableMap(map);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
