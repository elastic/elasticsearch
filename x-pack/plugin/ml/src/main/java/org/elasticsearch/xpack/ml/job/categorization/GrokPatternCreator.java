/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.grok.Grok;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Creates Grok patterns that will match all the examples in a given category_definition.
 *
 * The choice of field names is quite primitive.  The intention is that a human will edit these.
 */
public final class GrokPatternCreator {

    private static final Logger logger = LogManager.getLogger(GrokPatternCreator.class);

    private static final String PREFACE = "preface";
    private static final String EPILOGUE = "epilogue";
    private static final int MAX_RECURSE_DEPTH = 10;

    /**
     * The first match in this list will be chosen, so it needs to be ordered
     * such that more generic patterns come after more specific patterns.
     */
    private static final List<GrokPatternCandidate> ORDERED_CANDIDATE_GROK_PATTERNS = Arrays.asList(
            new GrokPatternCandidate("TOMCAT_DATESTAMP", "timestamp"),
            new GrokPatternCandidate("TIMESTAMP_ISO8601", "timestamp"),
            new GrokPatternCandidate("DATESTAMP_RFC822", "timestamp"),
            new GrokPatternCandidate("DATESTAMP_RFC2822", "timestamp"),
            new GrokPatternCandidate("DATESTAMP_OTHER", "timestamp"),
            new GrokPatternCandidate("DATESTAMP_EVENTLOG", "timestamp"),
            new GrokPatternCandidate("SYSLOGTIMESTAMP", "timestamp"),
            new GrokPatternCandidate("HTTPDATE", "timestamp"),
            new GrokPatternCandidate("CATALINA_DATESTAMP", "timestamp"),
            new GrokPatternCandidate("CISCOTIMESTAMP", "timestamp"),
            new GrokPatternCandidate("DATE", "date"),
            new GrokPatternCandidate("TIME", "time"),
            new GrokPatternCandidate("LOGLEVEL", "loglevel"),
            new GrokPatternCandidate("URI", "uri"),
            new GrokPatternCandidate("UUID", "uuid"),
            new GrokPatternCandidate("MAC", "macaddress"),
            // Can't use \b as the breaks, because slashes are not "word" characters
            new GrokPatternCandidate("PATH", "path", "(?<!\\w)", "(?!\\w)"),
            new GrokPatternCandidate("EMAILADDRESS", "email"),
            // TODO: would be nice to have IPORHOST here, but HOST matches almost all words
            new GrokPatternCandidate("IP", "ipaddress"),
            // This already includes pre/post break conditions
            new GrokPatternCandidate("QUOTEDSTRING", "field", "", ""),
            // Disallow +, - and . before numbers, as well as "word" characters, otherwise we'll pick
            // up numeric suffices too eagerly
            new GrokPatternCandidate("NUMBER", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\d)"),
            new GrokPatternCandidate("BASE16NUM", "field", "(?<![\\w.+-])", "(?![\\w+-]|\\.\\w)")
            // TODO: also unfortunately can't have USERNAME in the list as it matches too broadly
            // Fixing these problems with overly broad matches would require some extra intelligence
            // to be added to remove inappropriate matches.  One idea would be to use a dictionary,
            // but that doesn't necessarily help as "jay" could be a username but is also a dictionary
            // word (plus there's the international headache with relying on dictionaries).  Similarly,
            // hostnames could also be dictionary words - I've worked on machines called "hippo" and
            // "scarf" in the past.  Another idea would be to look at the adjacent characters and
            // apply some heuristic based on those.
    );

    private GrokPatternCreator() {
    }

    /**
     * Given a category definition regex and a collection of examples from the category, return
     * a grok pattern that will match the category and pull out any likely fields.  The extracted
     * fields are given pretty generic names, but unique within the grok pattern provided.  The
     * expectation is that a user will adjust the extracted field names based on their domain
     * knowledge.
     */
    public static String findBestGrokMatchFromExamples(String jobId, String regex, Collection<String> examples) {

        // The first string in this array will end up being the empty string, and it doesn't correspond
        // to an "in between" bit.  Although it could be removed for "neatness", it actually makes the
        // loops below slightly neater if it's left in.
        //
        // E.g., ".*?cat.+?sat.+?mat.*" -> [ "", "cat", "sat", "mat" ]
        String[] fixedRegexBits = regex.split("\\.[*+]\\??");

        // If there are no fixed regex bits, that implies there would only be a single capture group
        // And that would mean a pretty uninteresting grok pattern
        if (fixedRegexBits.length == 0) {
            return regex;
        }
        // Create a pattern that will capture the bits in between the fixed parts of the regex
        //
        // E.g., ".*?cat.+?sat.+?mat.*" -> Pattern (.*?)cat(.+?)sat(.+?)mat(.*)
        Pattern exampleProcessor = Pattern.compile(regex.replaceAll("(\\.[*+]\\??)", "($1)"), Pattern.DOTALL);

        List<Collection<String>> groupsMatchesFromExamples = new ArrayList<>(fixedRegexBits.length);
        for (int i = 0; i < fixedRegexBits.length; ++i) {
            groupsMatchesFromExamples.add(new ArrayList<>(examples.size()));
        }
        for (String example : examples) {
            Matcher matcher = exampleProcessor.matcher(example);
            if (matcher.matches()) {
                assert matcher.groupCount() == fixedRegexBits.length;
                // E.g., if the input regex was ".*?cat.+?sat.+?mat.*" then the example
                // "the cat sat on the mat" will result in "the ", " ", " on the ", and ""
                // being added to the 4 "in between" collections in that order
                for (int groupNum = 1; groupNum <= matcher.groupCount(); ++groupNum) {
                    groupsMatchesFromExamples.get(groupNum - 1).add(matcher.group(groupNum));
                }
            } else {
                // We should never get here.  If we do it implies a bug in the original categorization,
                // as it's produced a regex that doesn't match the examples.
                assert matcher.matches() : exampleProcessor.pattern() + " did not match " + example;
                logger.error("[{}] Pattern [{}] did not match example [{}]", jobId, exampleProcessor.pattern(), example);
            }
        }

        Map<String, Integer> fieldNameCountStore = new HashMap<>();
        StringBuilder overallGrokPatternBuilder = new StringBuilder();
        // Finally, for each collection of "in between" bits we look for the best Grok pattern and incorporate
        // it into the overall Grok pattern that will match the each example in its entirety
        for (int inBetweenBitNum = 0; inBetweenBitNum < groupsMatchesFromExamples.size(); ++inBetweenBitNum) {
            // Remember (from the first comment in this method) that the first element in this array is
            // always the empty string
            overallGrokPatternBuilder.append(fixedRegexBits[inBetweenBitNum]);
            appendBestGrokMatchForStrings(jobId, fieldNameCountStore, overallGrokPatternBuilder, inBetweenBitNum == 0,
                    inBetweenBitNum == fixedRegexBits.length - 1, groupsMatchesFromExamples.get(inBetweenBitNum));
        }
        return overallGrokPatternBuilder.toString();
    }

    private static void appendBestGrokMatchForStrings(String jobId,
                                                      Map<String, Integer> fieldNameCountStore,
                                                      StringBuilder overallGrokPatternBuilder,
                                                      boolean isFirst,
                                                      boolean isLast,
                                                      Collection<String> mustMatchStrings,
                                                      int numRecurse) {

        GrokPatternCandidate bestCandidate = null;
        if (mustMatchStrings.isEmpty() == false) {
            for (GrokPatternCandidate candidate : ORDERED_CANDIDATE_GROK_PATTERNS) {
                if (mustMatchStrings.stream().allMatch(candidate.grok::match)) {
                    bestCandidate = candidate;
                    break;
                }
            }
        }

        if (bestCandidate == null || numRecurse >= MAX_RECURSE_DEPTH) {
            if (bestCandidate != null) {
                logger.warn("[{}] exited grok discovery early, reached max depth [{}]", jobId, MAX_RECURSE_DEPTH);
            }
            if (isLast) {
                overallGrokPatternBuilder.append(".*");
            } else if (isFirst || mustMatchStrings.stream().anyMatch(String::isEmpty)) {
                overallGrokPatternBuilder.append(".*?");
            } else {
                overallGrokPatternBuilder.append(".+?");
            }
        } else {
            Collection<String> prefaces = new ArrayList<>();
            Collection<String> epilogues = new ArrayList<>();
            populatePrefacesAndEpilogues(mustMatchStrings, bestCandidate.grok, prefaces, epilogues);
            appendBestGrokMatchForStrings(jobId,
                fieldNameCountStore,
                overallGrokPatternBuilder,
                isFirst,
                false,
                prefaces,
                numRecurse + 1);
            overallGrokPatternBuilder.append("%{").append(bestCandidate.grokPatternName).append(':')
                .append(buildFieldName(fieldNameCountStore, bestCandidate.fieldName)).append('}');
            appendBestGrokMatchForStrings(jobId,
                fieldNameCountStore,
                overallGrokPatternBuilder,
                false, isLast,
                epilogues,
                numRecurse + 1);
        }
    }


    /**
     * Given a collection of strings, work out which (if any) of the grok patterns we're allowed
     * to use matches it best.  Then append the appropriate grok language to represent that finding
     * onto the supplied string builder.
     */
    static void appendBestGrokMatchForStrings(String jobId,
                                              Map<String, Integer> fieldNameCountStore,
                                              StringBuilder overallGrokPatternBuilder,
                                              boolean isFirst,
                                              boolean isLast,
                                              Collection<String> mustMatchStrings) {
        appendBestGrokMatchForStrings(jobId, fieldNameCountStore, overallGrokPatternBuilder, isFirst, isLast, mustMatchStrings, 0);
    }

    /**
     * Given a collection of strings, and a grok pattern that matches some part of them all,
     * return collections of the bits that come before (prefaces) and after (epilogues) the
     * bit that matches.
     */
    static void populatePrefacesAndEpilogues(Collection<String> matchingStrings, Grok grok, Collection<String> prefaces,
                                             Collection<String> epilogues) {
        for (String s : matchingStrings) {
            Map<String, Object> captures = grok.captures(s);
            // If the pattern doesn't match then captures will be null.  But we expect this
            // method to only be called after validating that the pattern does match.
            assert captures != null;
            prefaces.add(captures.getOrDefault(PREFACE, "").toString());
            epilogues.add(captures.getOrDefault(EPILOGUE, "").toString());
        }
    }

    /**
     * The first time a particular field name is passed, simply return it.
     * The second time return it with "2" appended.
     * The third time return it with "3" appended.
     * Etc.
     */
    static String buildFieldName(Map<String, Integer> fieldNameCountStore, String fieldName) {
        Integer numberSeen = fieldNameCountStore.compute(fieldName, (k, v) -> 1 + ((v == null) ? 0 : v));
        if (numberSeen > 1) {
            return fieldName + numberSeen;
        } else {
            return fieldName;
        }
    }

    static class GrokPatternCandidate {

        final String grokPatternName;
        final String fieldName;
        final Grok grok;

        /**
         * Pre/post breaks default to \b, but this may not be appropriate for Grok patterns that start or
         * end with a non "word" character (i.e. letter, number or underscore).  For such patterns use one
         * of the other constructors.
         *
         * In cases where the Grok pattern defined by Logstash already includes conditions on what must
         * come before and after the match, use one of the other constructors and specify an empty string
         * for the pre and/or post breaks.
         * @param grokPatternName Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param fieldName       Name of the field to extract from the match.
         */
        GrokPatternCandidate(String grokPatternName, String fieldName) {
            this(grokPatternName, fieldName, "\\b", "\\b");
        }

        GrokPatternCandidate(String grokPatternName, String fieldName, String preBreak) {
            this(grokPatternName, fieldName, preBreak, "\\b");
        }

        /**
         * @param grokPatternName Name of the Grok pattern to try to match - must match one defined in Logstash.
         * @param fieldName       Name of the field to extract from the match.
         * @param preBreak        Only consider the match if it's broken from the previous text by this.
         * @param postBreak       Only consider the match if it's broken from the following text by this.
         */
        GrokPatternCandidate(String grokPatternName, String fieldName, String preBreak, String postBreak) {
            this.grokPatternName = grokPatternName;
            this.fieldName = fieldName;
            this.grok = new Grok(Grok.getBuiltinPatterns(), "%{DATA:" + PREFACE + "}" + preBreak + "%{" + grokPatternName + ":this}" +
                    postBreak + "%{GREEDYDATA:" + EPILOGUE + "}");
        }
    }
}
