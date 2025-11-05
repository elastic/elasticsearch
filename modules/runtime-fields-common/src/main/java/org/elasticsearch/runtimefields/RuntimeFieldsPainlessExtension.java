/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.runtimefields;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistInstanceBinding;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;
import org.elasticsearch.script.AbstractFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptModule;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The painless extension that adds the necessary whitelist for grok and dissect to all the existing runtime fields contexts.
 */
public class RuntimeFieldsPainlessExtension implements PainlessExtension {
    private final List<Whitelist> whitelists;

    /**
     * Default constructor required by ServiceProvider but not used.
     * <p>
     * This constructor exists to satisfy module-info requirements but should not be called directly.
     * Use {@link #RuntimeFieldsPainlessExtension(RuntimeFieldsCommonPlugin)} instead.
     * </p>
     *
     * @throws UnsupportedOperationException always, as this constructor is not supported
     */
    // we don't use ServiceProvider directly, but module-info wants this
    public RuntimeFieldsPainlessExtension() {
        throw new UnsupportedOperationException();
    }

    /**
     * Constructs a new RuntimeFieldsPainlessExtension with grok and dissect support.
     * <p>
     * This constructor creates Painless whitelists that expose grok and dissect functionality
     * to runtime field scripts. The grok helper from the plugin is bound as an instance binding,
     * making it available for pattern compilation in Painless scripts.
     * </p>
     *
     * @param plugin the runtime fields common plugin providing the grok helper
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Constructor is called automatically during plugin initialization
     * RuntimeFieldsCommonPlugin plugin = new RuntimeFieldsCommonPlugin(settings);
     * RuntimeFieldsPainlessExtension extension = new RuntimeFieldsPainlessExtension(plugin);
     * }</pre>
     */
    public RuntimeFieldsPainlessExtension(RuntimeFieldsCommonPlugin plugin) {
        Whitelist commonWhitelist = WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "common_whitelist.txt");
        Whitelist grokWhitelist = new Whitelist(
            commonWhitelist.classLoader,
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new WhitelistInstanceBinding(
                    AbstractFieldScript.class.getCanonicalName(),
                    plugin.grokHelper(),
                    "grok",
                    NamedGroupExtractor.class.getName(),
                    List.of(String.class.getName()),
                    List.of(CompileTimeOnlyAnnotation.INSTANCE)
                )
            )
        );
        this.whitelists = List.of(commonWhitelist, grokWhitelist);
    }

    /**
     * Returns the Painless whitelists for runtime field script contexts.
     * <p>
     * This method provides whitelists containing grok, dissect, and related functionality
     * for all runtime field script contexts. The whitelists enable runtime field scripts
     * to use pattern matching and text extraction features.
     * </p>
     *
     * @return an immutable map of script contexts to their whitelists
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Painless during script compilation
     * Map<ScriptContext<?>, List<Whitelist>> whitelists = extension.getContextWhitelists();
     * // Runtime field scripts can now use:
     * // - grok("%{WORD:name}").extract(input)
     * // - dissect("%{name} %{age}").extract(input)
     * }</pre>
     */
    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        Map<ScriptContext<?>, List<Whitelist>> whiteLists = new HashMap<>();
        for (ScriptContext<?> scriptContext : ScriptModule.RUNTIME_FIELDS_CONTEXTS) {
            whiteLists.put(scriptContext, whitelists);
        }
        return Collections.unmodifiableMap(whiteLists);
    }
}
