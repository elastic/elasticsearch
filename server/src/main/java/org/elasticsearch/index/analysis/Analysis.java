/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.et.EstonianAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sr.SerbianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.CSVUtil;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;

public class Analysis {

    private static final Logger logger = LogManager.getLogger(Analysis.class);

    public static CharArraySet parseStemExclusion(Settings settings, CharArraySet defaultStemExclusion) {
        String value = settings.get("stem_exclusion");
        if ("_none_".equals(value)) {
            return CharArraySet.EMPTY_SET;
        }
        List<String> stemExclusion = settings.getAsList("stem_exclusion", null);
        if (stemExclusion != null) {
            // LUCENE 4 UPGRADE: Should be settings.getAsBoolean("stem_exclusion_case", false)?
            return new CharArraySet(stemExclusion, false);
        } else {
            return defaultStemExclusion;
        }
    }

    private static final Map<String, Set<?>> NAMED_STOP_WORDS = Map.ofEntries(
        entry("_arabic_", ArabicAnalyzer.getDefaultStopSet()),
        entry("_armenian_", ArmenianAnalyzer.getDefaultStopSet()),
        entry("_basque_", BasqueAnalyzer.getDefaultStopSet()),
        entry("_bengali_", BengaliAnalyzer.getDefaultStopSet()),
        entry("_brazilian_", BrazilianAnalyzer.getDefaultStopSet()),
        entry("_bulgarian_", BulgarianAnalyzer.getDefaultStopSet()),
        entry("_catalan_", CatalanAnalyzer.getDefaultStopSet()),
        entry("_czech_", CzechAnalyzer.getDefaultStopSet()),
        entry("_danish_", DanishAnalyzer.getDefaultStopSet()),
        entry("_dutch_", DutchAnalyzer.getDefaultStopSet()),
        entry("_english_", EnglishAnalyzer.getDefaultStopSet()),
        entry("_estonian_", EstonianAnalyzer.getDefaultStopSet()),
        entry("_finnish_", FinnishAnalyzer.getDefaultStopSet()),
        entry("_french_", FrenchAnalyzer.getDefaultStopSet()),
        entry("_galician_", GalicianAnalyzer.getDefaultStopSet()),
        entry("_german_", GermanAnalyzer.getDefaultStopSet()),
        entry("_greek_", GreekAnalyzer.getDefaultStopSet()),
        entry("_hindi_", HindiAnalyzer.getDefaultStopSet()),
        entry("_hungarian_", HungarianAnalyzer.getDefaultStopSet()),
        entry("_indonesian_", IndonesianAnalyzer.getDefaultStopSet()),
        entry("_irish_", IrishAnalyzer.getDefaultStopSet()),
        entry("_italian_", ItalianAnalyzer.getDefaultStopSet()),
        entry("_latvian_", LatvianAnalyzer.getDefaultStopSet()),
        entry("_lithuanian_", LithuanianAnalyzer.getDefaultStopSet()),
        entry("_norwegian_", NorwegianAnalyzer.getDefaultStopSet()),
        entry("_persian_", PersianAnalyzer.getDefaultStopSet()),
        entry("_portuguese_", PortugueseAnalyzer.getDefaultStopSet()),
        entry("_romanian_", RomanianAnalyzer.getDefaultStopSet()),
        entry("_russian_", RussianAnalyzer.getDefaultStopSet()),
        entry("_serbian_", SerbianAnalyzer.getDefaultStopSet()),
        entry("_sorani_", SoraniAnalyzer.getDefaultStopSet()),
        entry("_spanish_", SpanishAnalyzer.getDefaultStopSet()),
        entry("_swedish_", SwedishAnalyzer.getDefaultStopSet()),
        entry("_thai_", ThaiAnalyzer.getDefaultStopSet()),
        entry("_turkish_", TurkishAnalyzer.getDefaultStopSet())
    );

    public static CharArraySet parseWords(
        Environment env,
        Settings settings,
        String name,
        CharArraySet defaultWords,
        Map<String, Set<?>> namedWords,
        boolean ignoreCase
    ) {
        String value = settings.get(name);
        if (value != null) {
            if ("_none_".equals(value)) {
                return CharArraySet.EMPTY_SET;
            } else {
                return resolveNamedWords(settings.getAsList(name), namedWords, ignoreCase);
            }
        }
        List<String> pathLoadedWords = getWordList(env, settings, name);
        if (pathLoadedWords != null) {
            return resolveNamedWords(pathLoadedWords, namedWords, ignoreCase);
        }
        return defaultWords;
    }

    public static CharArraySet parseCommonWords(Environment env, Settings settings, CharArraySet defaultCommonWords, boolean ignoreCase) {
        return parseWords(env, settings, "common_words", defaultCommonWords, NAMED_STOP_WORDS, ignoreCase);
    }

    public static CharArraySet parseArticles(Environment env, Settings settings) {
        boolean articlesCase = settings.getAsBoolean("articles_case", false);
        return parseWords(env, settings, "articles", null, null, articlesCase);
    }

    public static CharArraySet parseStopWords(Environment env, Settings settings, CharArraySet defaultStopWords) {
        boolean stopwordsCase = settings.getAsBoolean("stopwords_case", false);
        return parseStopWords(env, settings, defaultStopWords, stopwordsCase);
    }

    public static CharArraySet parseStopWords(Environment env, Settings settings, CharArraySet defaultStopWords, boolean ignoreCase) {
        return parseWords(env, settings, "stopwords", defaultStopWords, NAMED_STOP_WORDS, ignoreCase);
    }

    private static CharArraySet resolveNamedWords(Collection<String> words, Map<String, Set<?>> namedWords, boolean ignoreCase) {
        if (namedWords == null) {
            return new CharArraySet(words, ignoreCase);
        }
        CharArraySet setWords = new CharArraySet(words.size(), ignoreCase);
        for (String word : words) {
            if (namedWords.containsKey(word)) {
                setWords.addAll(namedWords.get(word));
            } else {
                setWords.add(word);
            }
        }
        return setWords;
    }

    public static CharArraySet getWordSet(Environment env, Settings settings, String settingsPrefix) {
        List<String> wordList = getWordList(env, settings, settingsPrefix);
        if (wordList == null) {
            return null;
        }
        boolean ignoreCase = settings.getAsBoolean(settingsPrefix + "_case", false);
        return new CharArraySet(wordList, ignoreCase);
    }

    /**
     * Fetches a list of words from the specified settings file. The list should either be available at the key
     * specified by settingsPrefix or in a file specified by settingsPrefix + _path.
     *
     * @throws IllegalArgumentException
     *          If the word list cannot be found at either key.
     */
    public static List<String> getWordList(Environment env, Settings settings, String settingPrefix) {
        return getWordList(env, settings, settingPrefix + "_path", settingPrefix, true);
    }

    /**
     * Fetches a list of words from the specified settings file. The list should either be available at the key
     * specified by <code>settingList</code> or in a file specified by <code>settingPath</code>.
     *
     * @throws IllegalArgumentException
     *          If the word list cannot be found at either key.
     */
    public static List<String> getWordList(
        Environment env,
        Settings settings,
        String settingPath,
        String settingList,
        boolean removeComments
    ) {
        String wordListPath = settings.get(settingPath, null);

        if (wordListPath == null) {
            List<String> explicitWordList = settings.getAsList(settingList, null);
            if (explicitWordList == null) {
                return null;
            } else {
                return explicitWordList;
            }
        }

        final Path path = env.configDir().resolve(wordListPath);

        try {
            return loadWordList(path, removeComments);
        } catch (CharacterCodingException ex) {
            String message = Strings.format(
                "Unsupported character encoding detected while reading %s: %s - files must be UTF-8 encoded",
                settingPath,
                path
            );
            throw new IllegalArgumentException(message, ex);
        } catch (IOException ioe) {
            String message = Strings.format("IOException while reading %s: %s", settingPath, path);
            throw new IllegalArgumentException(message, ioe);
        } catch (SecurityException ace) {
            throw new IllegalArgumentException(Strings.format("Access denied trying to read file %s: %s", settingPath, path), ace);
        }
    }

    public static List<String> getWordList(
        Environment env,
        Settings settings,
        String settingPath,
        String settingList,
        String settingLenient,
        boolean removeComments,
        boolean checkDuplicate
    ) {
        boolean deduplicateDictionary = settings.getAsBoolean(settingLenient, false);
        final List<String> ruleList = getWordList(env, settings, settingPath, settingList, removeComments);
        if (ruleList != null && ruleList.isEmpty() == false && checkDuplicate) {
            return deDuplicateRules(ruleList, deduplicateDictionary == false);
        }
        return ruleList;
    }

    /**
     * This method checks for any duplicate rules in the provided ruleList. Each rule in the list is parsed with CSVUtil.parse
     * to separate the rule into individual components, represented as a String array. Only the first component from each rule
     * is considered in the duplication check.
     *
     * The method will ignore any line that starts with a '#' character, treating it as a comment.
     *
     * The check is performed by adding the first component of each rule into a HashSet (dup), which does not allow duplicates.
     * If the addition to the HashSet returns false, it means that item was already present in the set, indicating a duplicate.
     * In such a case, an IllegalArgumentException is thrown specifying the duplicate term and the line number in the original list.
     *
     * Optionally the function will return the deduplicated list
     *
     * @param ruleList The list of rules to check for duplicates.
     * @throws IllegalArgumentException If a duplicate rule is found.
     */
    private static List<String> deDuplicateRules(List<String> ruleList, boolean failOnDuplicate) {
        Set<String> duplicateKeys = new HashSet<>();
        List<String> deduplicatedList = new ArrayList<>();
        for (int lineNum = 0; lineNum < ruleList.size(); lineNum++) {
            String line = ruleList.get(lineNum);
            // ignore lines beginning with # as those are comments
            if (line.startsWith("#") == false) {
                String[] values = CSVUtil.parse(line);
                if (duplicateKeys.add(values[0]) == false) {
                    if (failOnDuplicate) {
                        throw new IllegalArgumentException(
                            "Found duplicate term [" + values[0] + "] in user dictionary " + "at line [" + (lineNum + 1) + "]"
                        );
                    } else {
                        logger.warn("Ignoring duplicate term [" + values[0] + "] in user dictionary " + "at line [" + (lineNum + 1) + "]");
                    }
                } else {
                    deduplicatedList.add(line);
                }
            } else {
                deduplicatedList.add(line);
            }
        }

        return Collections.unmodifiableList(deduplicatedList);
    }

    private static List<String> loadWordList(Path path, boolean removeComments) throws IOException {
        final List<String> result = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String word;
            while ((word = br.readLine()) != null) {
                if (Strings.hasText(word) == false) {
                    continue;
                }
                if (removeComments == false || word.startsWith("#") == false) {
                    result.add(word.trim());
                }
            }
        }
        return result;
    }

    /**
     * @return null If no settings set for "settingsPrefix" then return <code>null</code>.
     * @throws IllegalArgumentException
     *          If the Reader can not be instantiated.
     */
    public static Reader getReaderFromFile(Environment env, String filePath, String settingPrefix) {
        if (filePath == null) {
            return null;
        }
        final Path path = env.configDir().resolve(filePath);
        try {
            return Files.newBufferedReader(path, StandardCharsets.UTF_8);
        } catch (CharacterCodingException ex) {
            String message = String.format(
                Locale.ROOT,
                "Unsupported character encoding detected while reading %s_path: %s files must be UTF-8 encoded",
                settingPrefix,
                path.toString()
            );
            throw new IllegalArgumentException(message, ex);
        } catch (IOException ioe) {
            String message = String.format(Locale.ROOT, "IOException while reading %s_path: %s", settingPrefix, path.toString());
            throw new IllegalArgumentException(message, ioe);
        }
    }

    public static Reader getReaderFromIndex(
        String synonymsSet,
        SynonymsManagementAPIService synonymsManagementAPIService,
        boolean ignoreMissing
    ) {
        final PlainActionFuture<PagedResult<SynonymRule>> synonymsLoadingFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.getSynonymSetRules(synonymsSet, synonymsLoadingFuture);

        PagedResult<SynonymRule> results;

        try {
            results = synonymsLoadingFuture.actionGet();
        } catch (Exception e) {
            if (ignoreMissing == false) {
                throw e;
            }

            boolean notFound = e instanceof ResourceNotFoundException;
            String message = String.format(
                Locale.ROOT,
                "Synonyms set %s %s. Synonyms will not be applied to search results on indices that use this synonym set",
                synonymsSet,
                notFound ? "not found" : "could not be loaded"
            );

            if (notFound) {
                logger.warn(message);
            } else {
                logger.error(message, e);
            }

            results = new PagedResult<>(0, new SynonymRule[0]);
        }

        SynonymRule[] synonymRules = results.pageResults();
        StringBuilder sb = new StringBuilder();
        for (SynonymRule synonymRule : synonymRules) {
            sb.append(synonymRule.synonyms()).append(System.lineSeparator());
        }
        return new StringReader(sb.toString());
    }

}
