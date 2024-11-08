/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.redact;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.grok.GrokCaptureExtracter;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.grok.PatternBank;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackField;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Region;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * An ingest processor for redacting (obscuring) input text.
 * Uses Grok patterns to match text in the input, matches are
 * then replaced with redacted text.
 */
public class RedactProcessor extends AbstractProcessor {

    public static final LicensedFeature.Momentary REDACT_PROCESSOR_FEATURE = LicensedFeature.momentary(
        null,
        XPackField.REDACT_PROCESSOR,
        License.OperationMode.PLATINUM
    );

    public static final String TYPE = "redact";

    private static final Logger logger = LogManager.getLogger(RedactProcessor.class);

    private static final String DEFAULT_REDACTED_START = "<";
    private static final String DEFAULT_REDACTED_END = ">";

    protected static final String REDACT_KEY = "_redact";
    protected static final String IS_REDACTED_KEY = "_is_redacted";
    protected static final String METADATA_PATH_REDACT = IngestDocument.INGEST_KEY + "." + REDACT_KEY;
    // indicates if document has been redacted, path: _ingest._redact._is_redacted
    protected static final String METADATA_PATH_REDACT_IS_REDACTED = METADATA_PATH_REDACT + "." + IS_REDACTED_KEY;

    private final String redactField;
    private final List<Grok> groks;
    private final boolean ignoreMissing;

    private final String redactedStartToken;
    private final String redactedEndToken;

    private final XPackLicenseState licenseState;
    private final boolean skipIfUnlicensed;

    private final boolean traceRedact;

    RedactProcessor(
        String tag,
        String description,
        PatternBank patternBank,
        List<String> matchPatterns,
        String redactField,
        boolean ignoreMissing,
        String redactedStartToken,
        String redactedEndToken,
        MatcherWatchdog matcherWatchdog,
        XPackLicenseState licenseState,
        boolean skipIfUnlicensed,
        boolean traceRedact
    ) {
        super(tag, description);
        this.redactField = redactField;
        this.redactedStartToken = redactedStartToken;
        this.redactedEndToken = redactedEndToken;
        this.groks = new ArrayList<>(matchPatterns.size());
        for (var matchPattern : matchPatterns) {
            this.groks.add(new Grok(patternBank, matchPattern, matcherWatchdog, logger::debug));
        }
        this.ignoreMissing = ignoreMissing;
        // Joni warnings are only emitted on an attempt to match, and the warning emitted for every call to match which is too verbose
        // so here we emit a warning (if there is one) to the logfile at warn level on construction / processor creation.
        if (matchPatterns.isEmpty() == false) {
            new Grok(patternBank, matchPatterns.get(0), matcherWatchdog, logger::warn).match("___nomatch___");
        }
        this.licenseState = licenseState;
        this.skipIfUnlicensed = skipIfUnlicensed;
        this.traceRedact = traceRedact;
    }

    @Override
    public void extraValidation() throws Exception {
        // post-creation license check, this is exercised at rest/transport time
        if (skipIfUnlicensed == false && REDACT_PROCESSOR_FEATURE.check(licenseState) == false) {
            String message = LicenseUtils.newComplianceException(REDACT_PROCESSOR_FEATURE.getName()).getMessage();
            throw newConfigurationException(TYPE, tag, "skip_if_unlicensed", message);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        // runtime license check, this runs for every document processed
        if (REDACT_PROCESSOR_FEATURE.check(licenseState) == false) {
            if (skipIfUnlicensed) {
                return ingestDocument;
            } else {
                throw LicenseUtils.newComplianceException(REDACT_PROCESSOR_FEATURE.getName());
            }
        }

        // Call with ignoreMissing = true so getFieldValue does not throw
        final String fieldValue = ingestDocument.getFieldValue(redactField, String.class, true);

        if (fieldValue == null && ignoreMissing) {
            return ingestDocument;
        } else if (fieldValue == null) {
            throw new IllegalArgumentException("field [" + redactField + "] is null or missing");
        }

        try {
            String redacted = matchRedact(fieldValue, groks, redactedStartToken, redactedEndToken);
            ingestDocument.setFieldValue(redactField, redacted);
            updateMetadataIfNecessary(ingestDocument, fieldValue, redacted);

            return ingestDocument;
        } catch (RuntimeException e) {
            // grok throws a RuntimeException when the watchdog interrupts the match
            throw new ElasticsearchTimeoutException("Grok pattern matching timed out", e);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    List<Grok> getGroks() {
        return groks;
    }

    boolean getSkipIfUnlicensed() {
        return skipIfUnlicensed;
    }

    // exposed for testing
    static String matchRedact(String fieldValue, List<Grok> groks) {
        return matchRedact(fieldValue, groks, DEFAULT_REDACTED_START, DEFAULT_REDACTED_END);
    }

    /**
     * Finds all matches for each of the {@code groks} in {@code fieldValue} and
     * replaces the matched text with Grok pattern name.
     *
     * @param fieldValue Text to redact
     * @param groks Groks to match
     * @param redactedStartToken Matched and redacted regions are started with this token
     * @param redactedEndToken Matched and redacted regions are ended with this token
     * @return The redacted text
     */
    static String matchRedact(String fieldValue, List<Grok> groks, String redactedStartToken, String redactedEndToken) {
        byte[] utf8Bytes = fieldValue.getBytes(StandardCharsets.UTF_8);

        RegionTrackingMatchExtractor extractor = new RegionTrackingMatchExtractor();
        for (var grok : groks) {
            String patternName = grok.captureConfig().get(0).name();
            extractor.setCurrentPatternName(patternName);
            matchRepeat(grok, utf8Bytes, extractor);
        }

        if (extractor.replacementPositions.isEmpty()) {
            // no matches, nothing to redact
            return fieldValue;
        }
        return extractor.redactMatches(utf8Bytes, redactedStartToken, redactedEndToken);
    }

    private static void matchRepeat(Grok grok, byte[] utf8Bytes, RegionTrackingMatchExtractor extractor) {
        Matcher matcher = grok.getCompiledExpression().matcher(utf8Bytes, 0, utf8Bytes.length);
        int result;
        int offset = 0;
        int length = utf8Bytes.length;

        do {
            result = matcher.search(offset, length, Option.DEFAULT);
            if (result < 0) {
                break;
            }

            extractor.extract(utf8Bytes, 0, matcher.getEagerRegion());

            if (matcher.getEnd() == offset) {
                ++offset;
            } else {
                offset = matcher.getEnd();
            }

        } while (offset != length);
    }

    private void updateMetadataIfNecessary(IngestDocument ingestDocument, String fieldValue, String redacted) {
        if (traceRedact == false || fieldValue == null) {
            return;
        }

        Boolean isRedactedMetadata = ingestDocument.getFieldValue(METADATA_PATH_REDACT_IS_REDACTED, Boolean.class, true);
        boolean alreadyRedacted = Boolean.TRUE.equals(isRedactedMetadata);
        boolean isRedacted = fieldValue.equals(redacted) == false;

        // document newly redacted
        if (alreadyRedacted == false && isRedacted) {
            ingestDocument.setFieldValue(METADATA_PATH_REDACT_IS_REDACTED, true);
        }
    }

    /**
     * A Grok capture extractor which tracks matched regions
     * and the Grok pattern name for redaction later.
     */
    static class RegionTrackingMatchExtractor implements GrokCaptureExtracter {

        static class Replacement {
            int start;
            int end;
            final String patternName;

            Replacement(int start, int end, String patternName) {
                this.start = start;
                this.end = end;
                this.patternName = patternName;
            }

            int length() {
                return end - start;
            }

            @Override
            public String toString() {
                return "Replacement{" + "start=" + start + ", end=" + end + ", patternName='" + patternName + '\'' + '}';
            }
        }

        private final List<Replacement> replacementPositions;
        private String patternName;

        RegionTrackingMatchExtractor() {
            replacementPositions = new ArrayList<>();
        }

        void setCurrentPatternName(String patternName) {
            this.patternName = patternName;
        }

        @Override
        public void extract(byte[] utf8Bytes, int offset, Region region) {
            assert patternName != null;

            int number = 0;
            int matchOffset = offset + region.beg[number];
            int matchEnd = offset + region.end[number];
            replacementPositions.add(new Replacement(matchOffset, matchEnd, patternName));
        }

        /**
         * Replaces the matched regions in the original input text (passed as UTF8 bytes)
         * with the Grok matches. Each match is replaced by {@code redactStartToken}
         * followed by the Grok pattern name then suffixed by {@code redactEndToken}.
         *
         * Special care is taken to detect and manage regions that overlap, i.e. where
         * more than 1 Grok pattern has matched a piece of text. Where regions overlap
         * the pattern name of the longest matching regions is used for the replacement.
         *
         * @param utf8Bytes The original text as UTF8
         * @param redactStartToken The token to prefix the matched and redacted pattern name with
         * @param redactEndToken The token to suffix the matched and redacted pattern name with
         * @return A String with the matched regions redacted
         */
        String redactMatches(byte[] utf8Bytes, String redactStartToken, String redactEndToken) {
            var merged = mergeOverlappingReplacements(replacementPositions);
            int longestPatternName = merged.stream().mapToInt(r -> r.patternName.getBytes(StandardCharsets.UTF_8).length).max().getAsInt();

            int maxPossibleLength = longestPatternName * merged.size() + utf8Bytes.length;
            byte[] redact = new byte[maxPossibleLength];

            int readOffset = 0;
            int writeOffset = 0;
            for (var rep : merged) {
                int numBytesToWrite = rep.start - readOffset;
                System.arraycopy(utf8Bytes, readOffset, redact, writeOffset, numBytesToWrite);
                readOffset = rep.end;

                writeOffset = writeOffset + numBytesToWrite;

                byte[] replacementText = (redactStartToken + rep.patternName + redactEndToken).getBytes(StandardCharsets.UTF_8);
                System.arraycopy(replacementText, 0, redact, writeOffset, replacementText.length);

                writeOffset = writeOffset + replacementText.length;
            }

            int numBytesToWrite = utf8Bytes.length - readOffset;
            System.arraycopy(utf8Bytes, readOffset, redact, writeOffset, numBytesToWrite);
            writeOffset = writeOffset + numBytesToWrite;

            return new String(redact, 0, writeOffset, StandardCharsets.UTF_8);
        }

        /**
         * If {@code replacementPositions} contains overlapping regions
         * merge the overlaps.
         *
         * The strategy is to first sort the replacements by start position
         * then find and merge overlapping regions.
         * @param replacementPositions Found replacements, some of which may overlap
         * @return List of replacements ordered by start position
         */
        static List<Replacement> mergeOverlappingReplacements(List<Replacement> replacementPositions) {
            if (replacementPositions.size() == 1) {
                return replacementPositions;
            }

            List<Replacement> result = new ArrayList<>();
            // sort by start position
            replacementPositions.sort(Comparator.comparingInt(a -> a.start));
            int current = 0;
            int next = 1;
            while (current < replacementPositions.size()) {
                var head = replacementPositions.get(current);
                if (next >= replacementPositions.size() || head.end < replacementPositions.get(next).start) {
                    // Either current points to the last item in the list or
                    // there is no overlap and nothing to merge.
                    // Add the item to the result list
                    result.add(head);
                } else {
                    // Overlapping regions and the overlaps can be transitive
                    int previousRegionEnd;
                    do {
                        previousRegionEnd = replacementPositions.get(next).end;
                        next++;
                    } while (next < replacementPositions.size() && previousRegionEnd >= replacementPositions.get(next).start);

                    // merge into a single replacement
                    result.add(mergeLongestRegion(replacementPositions.subList(current, next)));
                }

                current = next;
                next++;
            }
            return result;
        }

        /**
         * Merge overlapping replacement regions into a single replacement.
         * The name of the result comes from the longest replacement
         *
         * @param replacementPositions Must be sorted by start position
         * @return Merged Replacement
         */
        static Replacement mergeLongestRegion(List<Replacement> replacementPositions) {
            assert replacementPositions.size() > 1;

            int longestIndex = 0;
            int endPos = replacementPositions.get(0).end;
            int maxLength = replacementPositions.get(0).length();

            for (int i = 1; i < replacementPositions.size(); i++) {
                if (replacementPositions.get(i).length() > maxLength) {
                    maxLength = replacementPositions.get(i).length();
                    longestIndex = i;
                }
                endPos = Math.max(endPos, replacementPositions.get(i).end);
            }

            return new Replacement(replacementPositions.get(0).start, endPos, replacementPositions.get(longestIndex).patternName);
        }
    }

    public static final class Factory implements Processor.Factory {

        private final XPackLicenseState licenseState;
        private final MatcherWatchdog matcherWatchdog;

        public Factory(XPackLicenseState licenseState, MatcherWatchdog matcherWatchdog) {
            this.licenseState = licenseState;
            this.matcherWatchdog = matcherWatchdog;
        }

        @Override
        public RedactProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String matchField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            List<String> matchPatterns = ConfigurationUtils.readList(TYPE, processorTag, config, "patterns");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);
            boolean skipIfUnlicensed = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "skip_if_unlicensed", false);

            String redactStart = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "prefix", DEFAULT_REDACTED_START);
            String redactEnd = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "suffix", DEFAULT_REDACTED_END);

            boolean traceRedact = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "trace_redact", false);

            if (matchPatterns == null || matchPatterns.isEmpty()) {
                throw newConfigurationException(TYPE, processorTag, "patterns", "List of patterns must not be empty");
            }
            Map<String, String> customPatternBank = ConfigurationUtils.readOptionalMap(TYPE, processorTag, config, "pattern_definitions");

            try {
                return new RedactProcessor(
                    processorTag,
                    description,
                    GrokBuiltinPatterns.ecsV1Patterns().extendWith(customPatternBank),
                    matchPatterns,
                    matchField,
                    ignoreMissing,
                    redactStart,
                    redactEnd,
                    matcherWatchdog,
                    licenseState,
                    skipIfUnlicensed,
                    traceRedact
                );
            } catch (Exception e) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "patterns",
                    "Invalid regex pattern found in: " + matchPatterns + ". " + e.getMessage()
                );
            }
        }
    }
}
