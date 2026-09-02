/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Shared compression library plugin for ESQL external data sources.
 *
 * <p>Hosted as a named module so it can be the single qualified-export target for
 * {@code org.elasticsearch.nativeaccess}'s public package — keeping the rest of the
 * native-access surface (process limits, mlock, exec sandbox, systemd hooks, raw memory
 * mapping) invisible to ESQL data source plugins.
 *
 * <p>The {@code requires} clauses on the bundled compression libraries
 * ({@code snappy.java}, {@code aircompressor}) pull those auto-modules into this plugin's
 * resolved module layer so the per-auto-module {@code load_native_libraries} entitlements
 * declared in {@code entitlement-policy.yaml} can be enabled for them — ES's
 * {@code PluginsLoader.enableNativeAccess} only enables native access on modules it can find
 * in the resolved layer.
 *
 * <p>The {@code transitive} qualifier covers two readability paths to extending plugins
 * (parquet, snappy, zstd, future orc/iceberg):
 * <ul>
 * <li><b>Today (extenders are unnamed):</b> reads propagate through
 *     {@code ExtendedPluginsClassLoader} parent-first delegation back to this plugin's
 *     full layer loader. Auto-modules transitively required here therefore stay visible
 *     even though no JPMS readability is involved on the consumer side.</li>
 * <li><b>Future (extenders become named modules):</b> a future child plugin that
 *     declares {@code requires org.elasticsearch.xpack.esql.datasource.compress} will
 *     automatically read the two transitively-required auto-modules without having
 *     to {@code requires} each of them itself.</li>
 * </ul>
 *
 * <p>The previous {@code requires transitive com.github.luben.zstd_jni} clause was removed
 * once both the {@code .csv.zst} / {@code .ndjson.zstd} streaming codec and Parquet's cold
 * {@code byte[]} decompressor were ported to Panama FFI's libzstd binding (see
 * {@code PanamaZstd}). zstd-jni is no longer on the production runtime classpath; the few
 * tests that still cross-check against zstd-jni's {@code ZstdInputStream}/{@code ZstdOutputStream}
 * pick it up via {@code testImplementation} only.
 *
 * <p><b>Adding a new native-loading compression library to this plugin requires four
 * coordinated edits</b>: (1) {@code implementation} dependency in {@code build.gradle};
 * (2) {@code requires transitive <auto-module-name>} in this file; (3) jar-name mapping in
 * the {@code AbstractDependenciesTask} block of {@code build.gradle}; (4) a per-auto-module
 * {@code load_native_libraries} stanza in {@code entitlement-policy.yaml}. Skipping (4)
 * trips an {@code assert false} in {@code PluginsLoader#enableNativeAccess}.
 */
module org.elasticsearch.xpack.esql.datasource.compress {
    requires org.elasticsearch.server;
    requires org.elasticsearch.nativeaccess;

    requires transitive snappy.java;
    requires transitive aircompressor;

    exports org.elasticsearch.xpack.esql.datasource.compress;
}
