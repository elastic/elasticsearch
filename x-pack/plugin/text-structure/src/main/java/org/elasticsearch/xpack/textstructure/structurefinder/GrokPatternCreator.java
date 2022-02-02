/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FieldStats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Creates Grok patterns that will match all provided sample messages.
 *
 * The choice of field names is quite primitive.  The intention is that a human will edit these.
 */
public final class GrokPatternCreator {

    private static final Logger logger = LogManager.getLogger(GrokPatternCreator.class);
    private static final Map<Character, Boolean> PUNCTUATION_OR_SPACE_NEEDS_ESCAPING;
    static {
        HashMap<Character, Boolean> punctuationOrSpaceNeedsEscaping = new HashMap<>();
        String punctuationAndSpaceCharacters = "\"'`‘’“”#@%=\\/|~:;,<>()[]{}«»^$*¿?¡!§¶ \t\n";
        String punctuationThatNeedsEscaping = "\\|()[]{}^$*?";
        punctuationAndSpaceCharacters.chars()
            .forEach(c -> punctuationOrSpaceNeedsEscaping.put((char) c, punctuationThatNeedsEscaping.indexOf(c) >= 0));
        PUNCTUATION_OR_SPACE_NEEDS_ESCAPING = Collections.unmodifiableMap(punctuationOrSpaceNeedsEscaping);
    }

    private static final String PREFACE = "preface";
    private static final String VALUE = "value";
    private static final String EPILOGUE = "epilogue";

    /**
     * Grok patterns that are designed to match the whole message, not just a part of it.
     */
    private static final List<FullMatchGrokPatternCandidate> FULL_MATCH_GROK_PATTERNS = Arrays.asList(
        FullMatchGrokPatternCandidate.fromGrokPatternName("BACULA_LOGLINE", "bts"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("CATALINALOG", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("COMBINEDAPACHELOG", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("COMMONAPACHELOG", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("ELB_ACCESS_LOG", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("HAPROXYHTTP", "syslog_timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("HAPROXYTCP", "syslog_timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("HTTPD20_ERRORLOG", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("HTTPD24_ERRORLOG", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("NAGIOSLOGLINE", "nagios_epoch"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("NETSCREENSESSIONLOG", "date"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("RAILS3", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("RUBY_LOGGER", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("SHOREWALL", "timestamp"),
        FullMatchGrokPatternCandidate.fromGrokPatternName("TOMCATLOG", "timestamp")
    );

    /**
     * The first match in this list will be chosen, so it needs to be ordered
     * such that more generic patterns come after more specific patterns.
     */
    private static final List<GrokPatternCandidate> ORDERED_CANDIDATE_GROK_PATTERNS = Arrays.asList(
        new ValueOnlyGrokPatternCandidate("TOMCAT_DATESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("TIMESTAMP_ISO8601", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_RFC822", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_RFC2822", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_OTHER", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP_EVENTLOG", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("HTTPDERROR_DATE", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("SYSLOGTIMESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("HTTPDATE", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("CATALINA_DATESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("CISCOTIMESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("DATESTAMP", "date", "extra_timestamp"),
        new ValueOnlyGrokPatternCandidate("LOGLEVEL", "keyword", "loglevel"),
        new ValueOnlyGrokPatternCandidate("URI", "keyword", "uri"),
        new ValueOnlyGrokPatternCandidate("UUID", "keyword", "uuid"),
        new ValueOnlyGrokPatternCandidate("MAC", "keyword", "macaddress"),
        // Can't use \b as the breaks, because slashes are not "word" characters
        new ValueOnlyGrokPatternCandidate("PATH", "keyword", "path", "(?<!\\w)", "(?!\\w)"),
        new ValueOnlyGrokPatternCandidate("EMAILADDRESS", "keyword", "email"),
        // TODO: would be nice to have IPORHOST here, but HOSTNAME matches almost all words
        new ValueOnlyGrokPatternCandidate("IP", "ip", "ipaddress"),
        new ValueOnlyGrokPatternCandidate("DATE", "date", "date"),
        // A time with no date cannot be stored in a field of type "date", hence "keyword"
        new ValueOnlyGrokPatternCandidate("TIME", "keyword", "time"),
        // This already includes pre/post break conditions
        new ValueOnlyGrokPatternCandidate("QUOTEDSTRING", "keyword", "field", "", ""),
        // Disallow +, - and . before numbers, as well as "word" characters, otherwise we'll pick
        // up numeric suffices too eagerly
        new ValueOnlyGrokPatternCandidate("INT", "long", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\d)"),
        new ValueOnlyGrokPatternCandidate("NUMBER", "double", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\d)"),
        new ValueOnlyGrokPatternCandidate("BASE16NUM", "keyword", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\w)")
        // TODO: also unfortunately can't have USERNAME in the list as it matches too broadly
        // Fixing these problems with overly broad matches would require some extra intelligence
        // to be added to remove inappropriate matches. One idea would be to use a dictionary,
        // but that doesn't necessarily help as "jay" could be a username but is also a dictionary
        // word (plus there's the international headache with relying on dictionaries). Similarly,
        // hostnames could also be dictionary words - I've worked on machines called "hippo" and
        // "scarf" in the past. Another idea would be to look at the adjacent characters and
        // apply some heuristic based on those.
    );

    /**
     * It is expected that the explanation will be shared with other code.
     * Both this class and other classes will update it.
     */
    private final List<String> explanation;
    private final Collection<String> sampleMessages;

    /**
     * It is expected that the mappings will be shared with other code.
     * Both this class and other classes will update it.
     */
    private final Map<String, Object> mappings;
    private final Map<String, FieldStats> fieldStats;
    private final Map<String, String> grokPatternDefinitions;
    private final Map<String, Integer> fieldNameCountStore = new HashMap<>();
    private final StringBuilder overallGrokPatternBuilder = new StringBuilder();
    private final TimeoutChecker timeoutChecker;

    /**
     *
     * @param explanation List of reasons for making decisions.  May contain items when passed and new reasons
     *                    can be appended by the methods of this class.
     * @param sampleMessages Sample messages that any Grok pattern found must match.
     * @param mappings Will be updated with mappings appropriate for the returned pattern, if non-<code>null</code>.
     * @param fieldStats Will be updated with field stats for the fields in the returned pattern, if non-<code>null</code>.
     * @param customGrokPatternDefinitions Custom Grok pattern definitions to add to the built-in ones.
     * @param timeoutChecker Will abort the operation if its timeout is exceeded.
     */
    public GrokPatternCreator(
        List<String> explanation,
        Collection<String> sampleMessages,
        Map<String, Object> mappings,
        Map<String, FieldStats> fieldStats,
        Map<String, String> customGrokPatternDefinitions,
        TimeoutChecker timeoutChecker
    ) {
        this.explanation = Objects.requireNonNull(explanation);
        this.sampleMessages = Collections.unmodifiableCollection(sampleMessages);
        this.mappings = mappings;
        this.fieldStats = fieldStats;
        if (customGrokPatternDefinitions.isEmpty()) {
            grokPatternDefinitions = Grok.getBuiltinPatterns(false);
        } else {
            grokPatternDefinitions = new HashMap<>(Grok.getBuiltinPatterns(false));
            grokPatternDefinitions.putAll(customGrokPatternDefinitions);
        }
        this.timeoutChecker = Objects.requireNonNull(timeoutChecker);
    }

    /**
     * This method attempts to find a Grok pattern that will match all of the sample messages in their entirety.
     * It will also update mappings and field stats if they are non-<code>null</code>.
     * @param timestampField If not <code>null</code> then the chosen Grok pattern must use this timestamp field.
     * @return A tuple of (time field name, Grok string), or <code>null</code> if no suitable Grok pattern was found.
     */
    public Tuple<String, String> findFullLineGrokPattern(String timestampField) {

        for (FullMatchGrokPatternCandidate candidate : FULL_MATCH_GROK_PATTERNS) {
            if (timestampField == null || timestampField.equals(candidate.getTimeField())) {
                if (candidate.matchesAll(sampleMessages, timeoutChecker)) {
                    return candidate.processMatch(explanation, sampleMessages, mappings, fieldStats, timeoutChecker);
                }
            }
        }

        return null;
    }

    /**
     * This method processes a user-supplied Grok pattern that will match all of the sample messages in their entirety.
     * It will also update mappings and field stats if they are non-<code>null</code>.
     * @param grokPattern The user supplied Grok pattern.
     * @param timestampField The name of the timestamp field within the Grok pattern.
     * @throws IllegalArgumentException If the supplied Grok pattern does not match the sample messages.
     */
    public void validateFullLineGrokPattern(String grokPattern, String timestampField) {

        FullMatchGrokPatternCandidate candidate = FullMatchGrokPatternCandidate.fromGrokPattern(
            grokPattern,
            timestampField,
            grokPatternDefinitions
        );
        if (candidate.matchesAll(sampleMessages, timeoutChecker)) {
            candidate.processMatch(explanation, sampleMessages, mappings, fieldStats, timeoutChecker);
        } else {
            throw new IllegalArgumentException("Supplied Grok pattern [" + grokPattern + "] does not match sample messages");
        }
    }

    /**
     * Build a Grok pattern that will match all of the sample messages in their entirety.
     * @param seedPatternName A pattern that has already been determined to match some portion of every sample message.
     * @param seedMapping     The mapping for the seed field.
     * @param seedFieldName   The field name to be used for the portion of every sample message that the seed pattern matches.
     * @return The built Grok pattern.
     */
    public String createGrokPatternFromExamples(String seedPatternName, Map<String, String> seedMapping, String seedFieldName) {

        overallGrokPatternBuilder.setLength(0);

        GrokPatternCandidate seedCandidate = new PrecalculatedMappingGrokPatternCandidate(
            seedPatternName,
            seedMapping,
            seedFieldName,
            grokPatternDefinitions
        );

        processCandidateAndSplit(seedCandidate, true, sampleMessages, false, 0, false, 0);

        return overallGrokPatternBuilder.toString().replace("\t", "\\t").replace("\n", "\\n");
    }

    /**
     * This is purely to allow unit tests to inspect the partial Grok pattern after testing implementation details.
     * It should not be used in production code.
     */
    StringBuilder getOverallGrokPatternBuilder() {
        return overallGrokPatternBuilder;
    }

    /**
     * Given a chosen Grok pattern and a collection of message snippets, split the snippets into the
     * matched section and the pieces before and after it.  Recurse to find more matches in the pieces
     * before and after and update the supplied string builder.
     */
    private void processCandidateAndSplit(
        GrokPatternCandidate chosenPattern,
        boolean isLast,
        Collection<String> snippets,
        boolean ignoreKeyValueCandidateLeft,
        int ignoreValueOnlyCandidatesLeft,
        boolean ignoreKeyValueCandidateRight,
        int ignoreValueOnlyCandidatesRight
    ) {

        Collection<String> prefaces = new ArrayList<>();
        Collection<String> epilogues = new ArrayList<>();
        String patternBuilderContent = chosenPattern.processCaptures(
            explanation,
            fieldNameCountStore,
            snippets,
            prefaces,
            epilogues,
            mappings,
            fieldStats,
            timeoutChecker
        );
        appendBestGrokMatchForStrings(false, prefaces, ignoreKeyValueCandidateLeft, ignoreValueOnlyCandidatesLeft);
        overallGrokPatternBuilder.append(patternBuilderContent);
        appendBestGrokMatchForStrings(isLast, epilogues, ignoreKeyValueCandidateRight, ignoreValueOnlyCandidatesRight);
    }

    /**
     * Given a collection of message snippets, work out which (if any) of the Grok patterns we're allowed
     * to use matches it best.  Then append the appropriate Grok language to represent that finding onto
     * the supplied string builder.
     */
    void appendBestGrokMatchForStrings(
        boolean isLast,
        Collection<String> snippets,
        boolean ignoreKeyValueCandidate,
        int ignoreValueOnlyCandidates
    ) {

        snippets = adjustForPunctuation(snippets);

        GrokPatternCandidate bestCandidate = null;
        if (snippets.isEmpty() == false) {
            GrokPatternCandidate kvCandidate = new KeyValueGrokPatternCandidate();
            if (ignoreKeyValueCandidate == false && kvCandidate.matchesAll(snippets)) {
                bestCandidate = kvCandidate;
            } else {
                ignoreKeyValueCandidate = true;
                for (GrokPatternCandidate candidate : ORDERED_CANDIDATE_GROK_PATTERNS.subList(
                    ignoreValueOnlyCandidates,
                    ORDERED_CANDIDATE_GROK_PATTERNS.size()
                )) {
                    if (candidate.matchesAll(snippets)) {
                        bestCandidate = candidate;
                        break;
                    }
                    ++ignoreValueOnlyCandidates;
                }
            }
        }

        if (bestCandidate == null) {
            if (isLast) {
                finalizeGrokPattern(snippets);
            } else {
                addIntermediateRegex(snippets);
            }
        } else {
            processCandidateAndSplit(
                bestCandidate,
                isLast,
                snippets,
                true,
                ignoreValueOnlyCandidates + (ignoreKeyValueCandidate ? 1 : 0),
                ignoreKeyValueCandidate,
                ignoreValueOnlyCandidates
            );
        }
    }

    /**
     * If the snippets supplied begin with more than 1 character of common punctuation or whitespace
     * then add all but the last of these characters to the overall pattern and remove them from the
     * snippets.
     * @param snippets Input snippets - not modified.
     * @return Output snippets, which will be a copy of the input snippets but with whatever characters
     *         were added to <code>overallPatternBuilder</code> removed from the beginning.
     */
    Collection<String> adjustForPunctuation(Collection<String> snippets) {

        assert snippets.isEmpty() == false;

        StringBuilder commonInitialPunctuation = new StringBuilder();

        for (String snippet : snippets) {

            if (commonInitialPunctuation.length() == 0) {
                for (int index = 0; index < snippet.length(); ++index) {
                    char ch = snippet.charAt(index);
                    if (PUNCTUATION_OR_SPACE_NEEDS_ESCAPING.get(ch) != null) {
                        commonInitialPunctuation.append(ch);
                    } else {
                        break;
                    }
                }
            } else {
                if (commonInitialPunctuation.length() > snippet.length()) {
                    commonInitialPunctuation.delete(snippet.length(), commonInitialPunctuation.length());
                }
                for (int index = 0; index < commonInitialPunctuation.length(); ++index) {
                    char ch = snippet.charAt(index);
                    if (ch != commonInitialPunctuation.charAt(index)) {
                        commonInitialPunctuation.delete(index, commonInitialPunctuation.length());
                        break;
                    }
                }
            }

            if (commonInitialPunctuation.length() <= 1) {
                return snippets;
            }
        }

        int numLiteralCharacters = commonInitialPunctuation.length() - 1;

        for (int index = 0; index < numLiteralCharacters; ++index) {
            char ch = commonInitialPunctuation.charAt(index);
            if (PUNCTUATION_OR_SPACE_NEEDS_ESCAPING.getOrDefault(ch, false)) {
                overallGrokPatternBuilder.append('\\');
            }
            overallGrokPatternBuilder.append(ch);
        }

        return snippets.stream().map(snippet -> snippet.substring(numLiteralCharacters)).collect(Collectors.toList());
    }

    /**
     * The first time a particular field name is passed, simply return it.
     * The second time return it with "2" appended.
     * The third time return it with "3" appended.
     * Etc.
     */
    static String buildFieldName(Map<String, Integer> fieldNameCountStore, String fieldName) {
        Integer numberSeen = fieldNameCountStore.compute(fieldName, (k, v) -> 1 + ((v == null) ? 0 : v));
        return (numberSeen > 1) ? fieldName + numberSeen : fieldName;
    }

    private void addIntermediateRegex(Collection<String> snippets) {
        addIntermediateRegex(overallGrokPatternBuilder, snippets);
    }

    public static void addIntermediateRegex(StringBuilder patternBuilder, Collection<String> snippets) {
        if (snippets.isEmpty()) {
            return;
        }

        List<String> others = new ArrayList<>(snippets);
        String driver = others.remove(others.size() - 1);

        boolean wildcardRequiredIfNonMatchFound = true;
        for (int i = 0; i < driver.length(); ++i) {
            char ch = driver.charAt(i);
            Boolean punctuationOrSpaceNeedsEscaping = PUNCTUATION_OR_SPACE_NEEDS_ESCAPING.get(ch);
            if (punctuationOrSpaceNeedsEscaping != null && others.stream().allMatch(other -> other.indexOf(ch) >= 0)) {
                if (wildcardRequiredIfNonMatchFound && others.stream().anyMatch(other -> other.indexOf(ch) > 0)) {
                    patternBuilder.append(".*?");
                }
                if (punctuationOrSpaceNeedsEscaping) {
                    patternBuilder.append('\\');
                }
                patternBuilder.append(ch);
                wildcardRequiredIfNonMatchFound = true;
                others = others.stream().map(other -> other.substring(other.indexOf(ch) + 1)).collect(Collectors.toList());
            } else if (wildcardRequiredIfNonMatchFound) {
                patternBuilder.append(".*?");
                wildcardRequiredIfNonMatchFound = false;
            }
        }

        if (wildcardRequiredIfNonMatchFound && others.stream().anyMatch(s -> s.isEmpty() == false)) {
            patternBuilder.append(".*?");
        }
    }

    private void finalizeGrokPattern(Collection<String> snippets) {
        if (snippets.stream().allMatch(String::isEmpty)) {
            return;
        }

        List<String> others = new ArrayList<>(snippets);
        String driver = others.remove(others.size() - 1);

        for (int i = 0; i < driver.length(); ++i) {
            char ch = driver.charAt(i);
            int driverIndex = i;
            Boolean punctuationOrSpaceNeedsEscaping = PUNCTUATION_OR_SPACE_NEEDS_ESCAPING.get(ch);
            if (punctuationOrSpaceNeedsEscaping != null
                && others.stream().allMatch(other -> other.length() > driverIndex && other.charAt(driverIndex) == ch)) {
                if (punctuationOrSpaceNeedsEscaping) {
                    overallGrokPatternBuilder.append('\\');
                }
                overallGrokPatternBuilder.append(ch);
                if (i == driver.length() - 1 && others.stream().allMatch(driver::equals)) {
                    return;
                }
            } else {
                break;
            }
        }

        overallGrokPatternBuilder.append(".*");
    }

    interface GrokPatternCandidate {

        /**
         * @return Does this Grok pattern candidate match all the snippets?
         */
        boolean matchesAll(Collection<String> snippets);

        /**
         * After it has been determined that this Grok pattern candidate matches a collection of strings,
         * return collections of the bits that come before (prefaces) and after (epilogues) the bit
         * that matches.  Also update mappings with the most appropriate field name and type, and
         * calculate field stats.
         * @return The string that needs to be incorporated into the overall Grok pattern for the line.
         */
        String processCaptures(
            List<String> explanation,
            Map<String, Integer> fieldNameCountStore,
            Collection<String> snippets,
            Collection<String> prefaces,
            Collection<String> epilogues,
            Map<String, Object> mappings,
            Map<String, FieldStats> fieldStats,
            TimeoutChecker timeoutChecker
        );
    }

    /**
     * A Grok pattern candidate that will match a single named Grok pattern.
     */
    static class ValueOnlyGrokPatternCandidate implements GrokPatternCandidate {

        private final String grokPatternName;
        private final Map<String, String> mapping;
        private final String fieldName;
        private final Grok grok;

        /**
         * Pre/post breaks default to \b, but this may not be appropriate for Grok patterns that start or
         * end with a non "word" character (i.e. letter, number or underscore).  For such patterns use one
         * of the other constructors.
         * <p>
         * In cases where the Grok pattern defined by Logstash already includes conditions on what must
         * come before and after the match, use one of the other constructors and specify an empty string
         * for the pre and/or post breaks.
         *
         * @param grokPatternName Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param mappingType     Data type for field in Elasticsearch mappings.
         * @param fieldName       Name of the field to extract from the match.
         */
        ValueOnlyGrokPatternCandidate(String grokPatternName, String mappingType, String fieldName) {
            this(
                grokPatternName,
                Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, mappingType),
                fieldName,
                "\\b",
                "\\b",
                Grok.getBuiltinPatterns(false)
            );
        }

        /**
         * @param grokPatternName        Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param mappingType            Data type for field in Elasticsearch mappings.
         * @param fieldName              Name of the field to extract from the match.
         * @param grokPatternDefinitions Definitions of Grok patterns to be used.
         */
        ValueOnlyGrokPatternCandidate(
            String grokPatternName,
            String mappingType,
            String fieldName,
            Map<String, String> grokPatternDefinitions
        ) {
            this(
                grokPatternName,
                Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, mappingType),
                fieldName,
                "\\b",
                "\\b",
                grokPatternDefinitions
            );
        }

        /**
         * @param grokPatternName Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param mappingType     Data type for field in Elasticsearch mappings.
         * @param fieldName       Name of the field to extract from the match.
         * @param preBreak        Only consider the match if it's broken from the previous text by this.
         * @param postBreak       Only consider the match if it's broken from the following text by this.
         */
        ValueOnlyGrokPatternCandidate(String grokPatternName, String mappingType, String fieldName, String preBreak, String postBreak) {
            this(
                grokPatternName,
                Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, mappingType),
                fieldName,
                preBreak,
                postBreak,
                Grok.getBuiltinPatterns(false)
            );
        }

        /**
         * @param grokPatternName        Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param mapping                Elasticsearch mapping for the field.
         * @param fieldName              Name of the field to extract from the match.
         * @param preBreak               Only consider the match if it's broken from the previous text by this.
         * @param postBreak              Only consider the match if it's broken from the following text by this.
         * @param grokPatternDefinitions Definitions of Grok patterns to be used.
         */
        ValueOnlyGrokPatternCandidate(
            String grokPatternName,
            Map<String, String> mapping,
            String fieldName,
            String preBreak,
            String postBreak,
            Map<String, String> grokPatternDefinitions
        ) {
            this.grokPatternName = Objects.requireNonNull(grokPatternName);
            this.mapping = Collections.unmodifiableMap(mapping);
            this.fieldName = Objects.requireNonNull(fieldName);
            // The (?m) here has the Ruby meaning, which is equivalent to (?s) in Java
            grok = new Grok(
                grokPatternDefinitions,
                "(?m)%{DATA:"
                    + PREFACE
                    + "}"
                    + Objects.requireNonNull(preBreak)
                    + "%{"
                    + grokPatternName
                    + ":"
                    + VALUE
                    + "}"
                    + Objects.requireNonNull(postBreak)
                    + "%{GREEDYDATA:"
                    + EPILOGUE
                    + "}",
                TimeoutChecker.watchdog,
                logger::warn
            );
        }

        @Override
        public boolean matchesAll(Collection<String> snippets) {
            return snippets.stream().allMatch(grok::match);
        }

        /**
         * Given a collection of strings, and a Grok pattern that matches some part of them all,
         * return collections of the bits that come before (prefaces) and after (epilogues) the
         * bit that matches.
         */
        @Override
        public String processCaptures(
            List<String> explanation,
            Map<String, Integer> fieldNameCountStore,
            Collection<String> snippets,
            Collection<String> prefaces,
            Collection<String> epilogues,
            Map<String, Object> mappings,
            Map<String, FieldStats> fieldStats,
            TimeoutChecker timeoutChecker
        ) {
            Collection<String> values = new ArrayList<>();
            for (String snippet : snippets) {
                Map<String, Object> captures = timeoutChecker.grokCaptures(grok, snippet, "full message Grok pattern field extraction");
                // If the pattern doesn't match then captures will be null
                if (captures == null) {
                    throw new IllegalStateException("[%{" + grokPatternName + "}] does not match snippet [" + snippet + "]");
                }
                prefaces.add(captures.getOrDefault(PREFACE, "").toString());
                values.add(captures.getOrDefault(VALUE, "").toString());
                epilogues.add(captures.getOrDefault(EPILOGUE, "").toString());
            }
            String adjustedFieldName = buildFieldName(fieldNameCountStore, fieldName);
            Map<String, String> adjustedMapping = mapping;
            // If the mapping is type "date" with no format, try to adjust it to include the format
            if (TextStructureUtils.DATE_MAPPING_WITHOUT_FORMAT.equals(adjustedMapping)) {
                try {
                    adjustedMapping = TextStructureUtils.findTimestampMapping(explanation, values, timeoutChecker);
                } catch (IllegalArgumentException e) {
                    // This feels like it shouldn't happen, but there may be some obscure edge case
                    // where it does, and in production it will cause less frustration to just return
                    // a mapping type of "date" with no format than to fail the whole analysis
                    assert e == null : e.getMessage();
                }
                timeoutChecker.check("mapping determination");
            }
            if (mappings != null) {
                mappings.put(adjustedFieldName, adjustedMapping);
            }
            if (fieldStats != null) {
                fieldStats.put(adjustedFieldName, TextStructureUtils.calculateFieldStats(adjustedMapping, values, timeoutChecker));
            }
            return "%{" + grokPatternName + ":" + adjustedFieldName + "}";
        }
    }

    /**
     * Unlike the {@link ValueOnlyGrokPatternCandidate} an object of this class is not immutable and not thread safe.
     * When a given object matches a set of strings it chooses a field name.  Then that same field name is used when
     * processing captures from the pattern.  Hence only a single thread may use any particular instance of this
     * class.
     */
    static class KeyValueGrokPatternCandidate implements GrokPatternCandidate {

        private static final Pattern KV_FINDER = Pattern.compile("\\b(\\w+)=[\\w.-]+");

        private String fieldName;

        @Override
        public boolean matchesAll(Collection<String> snippets) {
            Set<String> candidateNames = new LinkedHashSet<>();
            boolean isFirst = true;
            for (String snippet : snippets) {
                if (isFirst) {
                    Matcher matcher = KV_FINDER.matcher(snippet);
                    while (matcher.find()) {
                        candidateNames.add(matcher.group(1));
                    }
                    isFirst = false;
                } else {
                    candidateNames.removeIf(
                        candidateName -> Pattern.compile("\\b" + candidateName + "=[\\w.-]+").matcher(snippet).find() == false
                    );
                }
                if (candidateNames.isEmpty()) {
                    break;
                }
            }
            return (fieldName = candidateNames.stream().findFirst().orElse(null)) != null;
        }

        @Override
        public String processCaptures(
            List<String> explanation,
            Map<String, Integer> fieldNameCountStore,
            Collection<String> snippets,
            Collection<String> prefaces,
            Collection<String> epilogues,
            Map<String, Object> mappings,
            Map<String, FieldStats> fieldStats,
            TimeoutChecker timeoutChecker
        ) {
            if (fieldName == null) {
                throw new IllegalStateException("Cannot process KV matches until a field name has been determined");
            }
            Grok grok = new Grok(
                Grok.getBuiltinPatterns(false),
                "(?m)%{DATA:" + PREFACE + "}\\b" + fieldName + "=%{USER:" + VALUE + "}%{GREEDYDATA:" + EPILOGUE + "}",
                TimeoutChecker.watchdog,
                logger::warn
            );
            Collection<String> values = new ArrayList<>();
            for (String snippet : snippets) {
                Map<String, Object> captures = grok.captures(snippet);
                // If the pattern doesn't match then captures will be null
                if (captures == null) {
                    throw new IllegalStateException("[\\b" + fieldName + "=%{USER}] does not match snippet [" + snippet + "]");
                }
                prefaces.add(captures.getOrDefault(PREFACE, "").toString());
                values.add(captures.getOrDefault(VALUE, "").toString());
                epilogues.add(captures.getOrDefault(EPILOGUE, "").toString());
                timeoutChecker.check("full message Grok pattern field extraction");
            }
            String adjustedFieldName = buildFieldName(fieldNameCountStore, fieldName);
            Map<String, String> mapping = TextStructureUtils.guessScalarMapping(explanation, adjustedFieldName, values, timeoutChecker);
            timeoutChecker.check("mapping determination");
            if (mappings != null) {
                mappings.put(adjustedFieldName, mapping);
            }
            if (fieldStats != null) {
                fieldStats.put(adjustedFieldName, TextStructureUtils.calculateFieldStats(mapping, values, timeoutChecker));
            }
            return "\\b" + fieldName + "=%{USER:" + adjustedFieldName + "}";
        }
    }

    /**
     * A Grok pattern candidate that matches a single named Grok pattern but will not update mappings.
     */
    static class PrecalculatedMappingGrokPatternCandidate extends ValueOnlyGrokPatternCandidate {

        PrecalculatedMappingGrokPatternCandidate(
            String grokPatternName,
            Map<String, String> mapping,
            String fieldName,
            Map<String, String> grokPatternDefinitions
        ) {
            super(grokPatternName, mapping, fieldName, "\\b", "\\b", grokPatternDefinitions);
        }

        @Override
        public String processCaptures(
            List<String> explanation,
            Map<String, Integer> fieldNameCountStore,
            Collection<String> snippets,
            Collection<String> prefaces,
            Collection<String> epilogues,
            Map<String, Object> mappings,
            Map<String, FieldStats> fieldStats,
            TimeoutChecker timeoutChecker
        ) {
            return super.processCaptures(explanation, fieldNameCountStore, snippets, prefaces, epilogues, null, fieldStats, timeoutChecker);
        }
    }

    /**
     * Used to check whether a single Grok pattern matches every sample message in its entirety.
     */
    static class FullMatchGrokPatternCandidate {

        private final String grokPattern;
        private final String timeField;
        private final Grok grok;

        static FullMatchGrokPatternCandidate fromGrokPatternName(String grokPatternName, String timeField) {
            return new FullMatchGrokPatternCandidate("%{" + grokPatternName + "}", timeField, Grok.getBuiltinPatterns(false));
        }

        static FullMatchGrokPatternCandidate fromGrokPatternName(
            String grokPatternName,
            String timeField,
            Map<String, String> grokPatternDefinitions
        ) {
            return new FullMatchGrokPatternCandidate("%{" + grokPatternName + "}", timeField, grokPatternDefinitions);
        }

        static FullMatchGrokPatternCandidate fromGrokPattern(String grokPattern, String timeField) {
            return new FullMatchGrokPatternCandidate(grokPattern, timeField, Grok.getBuiltinPatterns(false));
        }

        static FullMatchGrokPatternCandidate fromGrokPattern(
            String grokPattern,
            String timeField,
            Map<String, String> grokPatternDefinitions
        ) {
            return new FullMatchGrokPatternCandidate(grokPattern, timeField, grokPatternDefinitions);
        }

        private FullMatchGrokPatternCandidate(String grokPattern, String timeField, Map<String, String> grokPatternDefinitions) {
            this.grokPattern = grokPattern;
            this.timeField = timeField;
            grok = new Grok(grokPatternDefinitions, grokPattern, TimeoutChecker.watchdog, logger::warn);
        }

        public String getTimeField() {
            return timeField;
        }

        public boolean matchesAll(Collection<String> sampleMessages, TimeoutChecker timeoutChecker) {
            for (String sampleMessage : sampleMessages) {
                if (grok.match(sampleMessage) == false) {
                    return false;
                }
                timeoutChecker.check("full message Grok pattern matching");
            }
            return true;
        }

        /**
         * This must only be called if {@link #matchesAll} returns <code>true</code>.
         * @return A tuple of (time field name, Grok string).
         */
        public Tuple<String, String> processMatch(
            List<String> explanation,
            Collection<String> sampleMessages,
            Map<String, Object> mappings,
            Map<String, FieldStats> fieldStats,
            TimeoutChecker timeoutChecker
        ) {

            if (grokPattern.startsWith("%{") && grokPattern.endsWith("}")) {
                explanation.add(
                    "A full message Grok pattern [" + grokPattern.substring(2, grokPattern.length() - 1) + "] looks appropriate"
                );
            }
            if (mappings != null || fieldStats != null) {
                Map<String, Collection<String>> valuesPerField = new HashMap<>();

                for (String sampleMessage : sampleMessages) {
                    Map<String, Object> captures = timeoutChecker.grokCaptures(
                        grok,
                        sampleMessage,
                        "full message Grok pattern field extraction"
                    );
                    // If the pattern doesn't match then captures will be null
                    if (captures == null) {
                        throw new IllegalStateException("[" + grokPattern + "] does not match snippet [" + sampleMessage + "]");
                    }
                    for (Map.Entry<String, Object> capture : captures.entrySet()) {

                        String fieldName = capture.getKey();
                        String fieldValue = capture.getValue().toString();
                        valuesPerField.compute(fieldName, (k, v) -> {
                            if (v == null) {
                                return new ArrayList<>(Collections.singletonList(fieldValue));
                            } else {
                                v.add(fieldValue);
                                return v;
                            }
                        });
                    }
                }

                for (Map.Entry<String, Collection<String>> valuesForField : valuesPerField.entrySet()) {
                    String fieldName = valuesForField.getKey();
                    Map<String, String> mapping = TextStructureUtils.guessScalarMapping(
                        explanation,
                        fieldName,
                        valuesForField.getValue(),
                        timeoutChecker
                    );
                    timeoutChecker.check("mapping determination");
                    // Exclude the time field because that will be dropped and replaced with @timestamp
                    if (mappings != null && fieldName.equals(timeField) == false) {
                        mappings.put(fieldName, mapping);
                    }
                    if (fieldStats != null) {
                        fieldStats.put(
                            fieldName,
                            TextStructureUtils.calculateFieldStats(mapping, valuesForField.getValue(), timeoutChecker)
                        );
                    }
                }
            }

            return new Tuple<>(timeField, grokPattern);
        }
    }
}
