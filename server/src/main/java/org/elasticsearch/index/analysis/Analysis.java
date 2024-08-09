/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.synonyms.SynonymRule;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;

public class Analysis {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(Analysis.class);
    private static final Logger logger = LogManager.getLogger(Analysis.class);

    public static void checkForDeprecatedVersion(String name, Settings settings) {
        String sVersion = settings.get("version");
        if (sVersion != null) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.ANALYSIS,
                "analyzer.version",
                "Setting [version] on analysis component [" + name + "] has no effect and is deprecated"
            );
        }
    }

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

        final Path path = env.configFile().resolve(wordListPath);

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
        } catch (AccessControlException ace) {
            throw new IllegalArgumentException(Strings.format("Access denied trying to read file %s: %s", settingPath, path), ace);
        }
    }

    public static void getWordListAsync(
        String indexName,
        Environment env,
        Settings settings,
        String settingPath,
        String settingList,
        boolean removeComments,
        WordListsIndexService wordListIndexService,
        ActionListener<List<String>> listener
    ) {
        String wordListPath = settings.get(settingPath, null);

        if (wordListPath == null) {
            List<String> explicitWordList = settings.getAsList(settingList, null);
            listener.onResponse(explicitWordList);
            return;
        }

        final URL pathAsUrl = tryToParsePathAsURL(wordListPath);
        if (pathAsUrl != null) {
            // Don't try to read data from URLs in the master update thread because you won't get the async results back in time to use them
            if (MasterService.isMasterUpdateThread() == false) {
                wordListIndexService.getWordListValue(indexName, wordListPath, new ActionListener<>() {
                    @Override
                    public void onResponse(String s) {
                        if (s == null) {
                            try {
                                s = readFile(pathAsUrl);
                            } catch (IOException e) {
                                listener.onFailure(
                                    new ElasticsearchStatusException("Unable to read file at " + settingPath, RestStatus.BAD_REQUEST, e)
                                );
                                return;
                            }

                            // TODO: Force indexing to complete before returning parsed word list to listener
                            wordListIndexService.putWordList(indexName, wordListPath, s, new ActionListener<>() {
                                @Override
                                public void onResponse(WordListsIndexService.PutWordListResult putWordListResult) {
                                    logger.debug("Indexed word list [" + wordListPath + "] for index [" + indexName + "]");
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.warn("Unable to index word list [" + wordListPath + "] for index [" + indexName + "]", e);
                                    // listener.onFailure(new ElasticsearchStatusException(
                                    // "Unable to index word list [" + wordListPath + "] for index [" + indexName + "]",
                                    // RestStatus.INTERNAL_SERVER_ERROR,
                                    // e
                                    // ));
                                }
                            });
                        }

                        try {
                            listener.onResponse(loadWordList(s, removeComments));
                        } catch (IOException e) {
                            listener.onFailure(
                                new ElasticsearchStatusException(
                                    "Unable to parse word list [" + wordListPath + "] for index [" + indexName + "]",
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    e
                                )
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                "Unable to get word list [" + wordListPath + "] for index [" + indexName + "]",
                                RestStatus.INTERNAL_SERVER_ERROR,
                                e
                            )
                        );
                    }
                });
            }
        } else {
            // Throw exceptions synchronously here so that master thread validation can pick them up
            final Path path = env.configFile().resolve(wordListPath);
            try {
                listener.onResponse(loadWordList(path, removeComments));
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
            } catch (AccessControlException ace) {
                throw new IllegalArgumentException(Strings.format("Access denied trying to read file %s: %s", settingPath, path), ace);
            }
        }
    }

    public static List<String> getWordList(
        Environment env,
        Settings settings,
        String settingPath,
        String settingList,
        boolean removeComments,
        boolean checkDuplicate
    ) {
        final List<String> ruleList = getWordList(env, settings, settingPath, settingList, removeComments);
        if (ruleList != null && ruleList.isEmpty() == false && checkDuplicate) {
            checkDuplicateRules(ruleList);
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
     * @param ruleList The list of rules to check for duplicates.
     * @throws IllegalArgumentException If a duplicate rule is found.
     */
    private static void checkDuplicateRules(List<String> ruleList) {
        Set<String> dup = new HashSet<>();
        int lineNum = 0;
        for (String line : ruleList) {
            // ignore comments
            if (line.startsWith("#") == false) {
                String[] values = CSVUtil.parse(line);
                if (dup.add(values[0]) == false) {
                    throw new IllegalArgumentException(
                        "Found duplicate term [" + values[0] + "] in user dictionary " + "at line [" + lineNum + "]"
                    );
                }
            }
            ++lineNum;
        }
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

    private static List<String> loadWordList(String value, boolean removeComments) throws IOException {
        final List<String> result = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new StringReader(value))) {
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
        final Path path = env.configFile().resolve(filePath);
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

    public static Reader getReaderFromIndex(String synonymsSet, SynonymsManagementAPIService synonymsManagementAPIService) {
        final PlainActionFuture<PagedResult<SynonymRule>> synonymsLoadingFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.getSynonymSetRules(synonymsSet, synonymsLoadingFuture);
        PagedResult<SynonymRule> results = synonymsLoadingFuture.actionGet();

        SynonymRule[] synonymRules = results.pageResults();
        StringBuilder sb = new StringBuilder();
        for (SynonymRule synonymRule : synonymRules) {
            sb.append(synonymRule.synonyms()).append(System.lineSeparator());
        }
        return new StringReader(sb.toString());
    }

    // TODO: How to handle file:// URLs?
    private static URL tryToParsePathAsURL(String path) {
        URL url = null;
        try {
            url = new URL(path);
        } catch (MalformedURLException e) {
            // OK to swallow this exception, it means the path is not a valid URL
        }

        return url;
    }

    private static Path downloadFile(URL url) throws IOException {
        String protocol = url.getProtocol();
        if (protocol.equals("http") == false && protocol.equals("https") == false) {
            throw new IllegalArgumentException("Cannot handle protocol [" + protocol + "]");
        }

        SpecialPermission.check();
        Path path = Files.createTempFile(UUIDs.randomBase64UUID(), null);
        try (FileOutputStream fileOutputStream = new FileOutputStream(path.toFile())) {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
                fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
                return null;
            });
        } catch (PrivilegedActionException e) {
            Files.deleteIfExists(path);
            throw (IOException) e.getCause();
        } catch (Exception e) {
            Files.deleteIfExists(path);
            throw e;
        }

        return path;
    }

    private static String readFile(URL url) throws IOException {
        String protocol = url.getProtocol();
        if (protocol.equals("http") == false && protocol.equals("https") == false) {
            throw new IllegalArgumentException("Cannot handle protocol [" + protocol + "]");
        }

        SpecialPermission.check();
        ReadableByteChannel readableByteChannel = null;
        StringBuilder sb = new StringBuilder();
        try {
            readableByteChannel = AccessController.doPrivileged(
                (PrivilegedExceptionAction<ReadableByteChannel>) () -> Channels.newChannel(url.openStream())
            );

            Reader reader = Channels.newReader(readableByteChannel, StandardCharsets.UTF_8);
            char[] arr = new char[1024];
            int charsRead;
            while ((charsRead = reader.read(arr, 0, arr.length)) != -1) {
                sb.append(arr, 0, charsRead);
            }
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        } finally {
            if (readableByteChannel != null) {
                readableByteChannel.close();
            }
        }

        return sb.toString();
    }

    // TODO: How to handle file:// URLs?
    private static URI tryToParsePathAsUri(String path) {
        URI uri = null;
        try {
            uri = new URI(path);
        } catch (URISyntaxException e) {
            // OK to swallow this exception, it means the path is not a valid URL
        }

        return uri;
    }

    private static Path downloadFile(URI uri) throws IOException, InterruptedException {
        String protocol = uri.getScheme();
        if (protocol.equals("http") == false && protocol.equals("https") == false) {
            throw new IllegalArgumentException("Cannot handle protocol [" + protocol + "]");
        }

        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder(uri).build();
        SpecialPermission.check();
        HttpResponse<InputStream> httpResponse;
        try {
            httpResponse = AccessController.doPrivileged(
                (PrivilegedExceptionAction<HttpResponse<InputStream>>) () -> httpClient.send(
                    httpRequest,
                    HttpResponse.BodyHandlers.ofInputStream()
                )
            );
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }

        Path path = Files.createTempFile(UUIDs.randomBase64UUID(), null);
        try (FileOutputStream fileOutputStream = new FileOutputStream(path.toFile()); InputStream body = httpResponse.body()) {
            fileOutputStream.write(body.readAllBytes());
        } catch (Exception e) {
            Files.deleteIfExists(path);
            throw e;
        }

        return path;
    }
}
