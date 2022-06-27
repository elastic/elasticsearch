/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

/**
 * Serves as a node level registry for hunspell dictionaries. This services expects all dictionaries to be located under
 * the {@code <path.conf>/hunspell} directory, where each locale has its dedicated sub-directory which holds the dictionary
 * files. For example, the dictionary files for {@code en_US} locale must be placed under {@code <path.conf>/hunspell/en_US}
 * directory.
 * <p>
 * The following settings can be set for each dictionary:
 * <ul>
 * <li>{@code ignore_case} - If true, dictionary matching will be case insensitive (defaults to {@code false})</li>
 * <li>{@code strict_affix_parsing} - Determines whether errors while reading a affix rules file will cause exception or simple be ignored
 *      (defaults to {@code true})</li>
 * </ul>
 * <p>
 * These settings can either be configured as node level configuration, such as:
 * <br><br>
 * <pre><code>
 *     indices.analysis.hunspell.dictionary.en_US.ignore_case: true
 *     indices.analysis.hunspell.dictionary.en_US.strict_affix_parsing: false
 * </code></pre>
 * <p>
 * or, as dedicated configuration per dictionary, placed in a {@code settings.yml} file under the dictionary directory. For
 * example, the following can be the content of the {@code <path.config>/hunspell/en_US/settings.yml} file:
 * <br><br>
 * <pre><code>
 *     ignore_case: true
 *     strict_affix_parsing: false
 * </code></pre>
 *
 * @see org.elasticsearch.index.analysis.HunspellTokenFilterFactory
 */
public class HunspellService {

    private static final Logger logger = LogManager.getLogger(HunspellService.class);

    public static final Setting<Boolean> HUNSPELL_LAZY_LOAD = Setting.boolSetting(
        "indices.analysis.hunspell.dictionary.lazy",
        Boolean.FALSE,
        Property.NodeScope
    );
    public static final Setting<Boolean> HUNSPELL_IGNORE_CASE = Setting.boolSetting(
        "indices.analysis.hunspell.dictionary.ignore_case",
        Boolean.FALSE,
        Property.NodeScope
    );
    public static final Setting<Settings> HUNSPELL_DICTIONARY_OPTIONS = Setting.groupSetting(
        "indices.analysis.hunspell.dictionary.",
        Property.NodeScope
    );
    private final ConcurrentHashMap<String, Dictionary> dictionaries = new ConcurrentHashMap<>();
    private final Map<String, Dictionary> knownDictionaries;
    private final boolean defaultIgnoreCase;
    private final Path hunspellDir;
    private final Function<String, Dictionary> loadingFunction;

    public HunspellService(final Settings settings, final Environment env, final Map<String, Dictionary> knownDictionaries)
        throws IOException {
        this.knownDictionaries = Collections.unmodifiableMap(knownDictionaries);
        this.hunspellDir = resolveHunspellDirectory(env);
        this.defaultIgnoreCase = HUNSPELL_IGNORE_CASE.get(settings);
        this.loadingFunction = (locale) -> {
            try {
                return loadDictionary(locale, settings, env);
            } catch (Exception e) {
                throw new IllegalStateException("failed to load hunspell dictionary for locale: " + locale, e);
            }
        };
        if (HUNSPELL_LAZY_LOAD.get(settings) == false) {
            scanAndLoadDictionaries();
        }

    }

    /**
     * Returns the hunspell dictionary for the given locale.
     *
     * @param locale The name of the locale
     */
    public Dictionary getDictionary(String locale) {
        Dictionary dictionary = knownDictionaries.get(locale);
        if (dictionary == null) {
            dictionary = dictionaries.computeIfAbsent(locale, loadingFunction);
        }
        return dictionary;
    }

    private static Path resolveHunspellDirectory(Environment env) {
        return env.configFile().resolve("hunspell");
    }

    /**
     * Scans the hunspell directory and loads all found dictionaries
     */
    private void scanAndLoadDictionaries() throws IOException {
        if (Files.isDirectory(hunspellDir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hunspellDir)) {
                for (Path file : stream) {
                    if (Files.isDirectory(file)) {
                        try (DirectoryStream<Path> inner = Files.newDirectoryStream(hunspellDir.resolve(file), "*.dic")) {
                            if (inner.iterator().hasNext()) { // just making sure it's indeed a dictionary dir
                                try {
                                    getDictionary(file.getFileName().toString());
                                } catch (Exception e) {
                                    // The cache loader throws unchecked exception (see #loadDictionary()),
                                    // here we simply report the exception and continue loading the dictionaries
                                    logger.error(() -> format("exception while loading dictionary %s", file.getFileName()), e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Loads the hunspell dictionary for the given local.
     *
     * @param locale       The locale of the hunspell dictionary to be loaded.
     * @param nodeSettings The node level settings
     * @param env          The node environment (from which the conf path will be resolved)
     * @return The loaded Hunspell dictionary
     * @throws Exception when loading fails (due to IO errors or malformed dictionary files)
     */
    private Dictionary loadDictionary(String locale, Settings nodeSettings, Environment env) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Loading hunspell dictionary [{}]...", locale);
        }
        Path dicDir = hunspellDir.resolve(locale);
        if (FileSystemUtils.isAccessibleDirectory(dicDir, logger) == false) {
            throw new ElasticsearchException(String.format(Locale.ROOT, "Could not find hunspell dictionary [%s]", locale));
        }

        // merging node settings with hunspell dictionary specific settings
        Settings dictSettings = HUNSPELL_DICTIONARY_OPTIONS.get(nodeSettings);
        nodeSettings = loadDictionarySettings(dicDir, dictSettings.getByPrefix(locale + "."));

        boolean ignoreCase = nodeSettings.getAsBoolean("ignore_case", defaultIgnoreCase);

        Path[] affixFiles = FileSystemUtils.files(dicDir, "*.aff");
        if (affixFiles.length == 0) {
            throw new ElasticsearchException(String.format(Locale.ROOT, "Missing affix file for hunspell dictionary [%s]", locale));
        }
        if (affixFiles.length != 1) {
            throw new ElasticsearchException(String.format(Locale.ROOT, "Too many affix files exist for hunspell dictionary [%s]", locale));
        }
        InputStream affixStream = null;

        Path[] dicFiles = FileSystemUtils.files(dicDir, "*.dic");
        List<InputStream> dicStreams = new ArrayList<>(dicFiles.length);
        try {

            for (int i = 0; i < dicFiles.length; i++) {
                dicStreams.add(Files.newInputStream(dicFiles[i]));
            }

            affixStream = Files.newInputStream(affixFiles[0]);

            try (Directory tmp = new NIOFSDirectory(env.tmpFile())) {
                return new Dictionary(tmp, "hunspell", affixStream, dicStreams, ignoreCase);
            }

        } catch (Exception e) {
            logger.error(() -> "Could not load hunspell dictionary [" + locale + "]", e);
            throw e;
        } finally {
            IOUtils.close(affixStream);
            IOUtils.close(dicStreams);
        }
    }

    /**
     * Each hunspell dictionary directory may contain a {@code settings.yml} which holds dictionary specific settings. Default
     * values for these settings are defined in the given default settings.
     *
     * @param dir      The directory of the dictionary
     * @param defaults The default settings for this dictionary
     * @return The resolved settings.
     */
    private static Settings loadDictionarySettings(Path dir, Settings defaults) throws IOException {
        Path file = dir.resolve("settings.yml");
        if (Files.exists(file)) {
            return Settings.builder().loadFromPath(file).put(defaults).build();
        }

        file = dir.resolve("settings.json");
        if (Files.exists(file)) {
            return Settings.builder().loadFromPath(file).put(defaults).build();
        }

        return defaults;
    }
}
