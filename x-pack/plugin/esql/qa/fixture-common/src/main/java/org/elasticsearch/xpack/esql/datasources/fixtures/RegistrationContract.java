/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * What the dataset registration endpoint must refuse, read from {@code dataset-registration-cases.properties}.
 *
 * <p>Separate from {@link FixtureDimensions} because the claims are a different shape. A dimension is
 * something to CROSS: the contract records which pairs are worth generating together, and the crossing
 * is derived from those verdicts. A refusal names the combination it is about, so the case IS the
 * combination and there is nothing to derive -- declaring one as a dimension would demand a verdict
 * against every other dimension to reach a cell the case already states outright.
 *
 * <p>Combinations, not single settings. Most refusals here pin one setting, but the ones worth having
 * pin two that are each valid alone: an error budget with no error mode, a probe window that is only
 * too wide at that probe count. A contract of one setting per case cannot express either.
 *
 * <p>The load-bearing field is the message. Registration performs no I/O, so the endpoint's whole
 * contract is which settings it can decide about and what it says when it decides against one -- and a
 * test that checks only the status cannot tell a refusal of the setting it varied from a refusal of
 * something it did not. Those two are the same green tick everywhere downstream.
 */
public final class RegistrationContract {

    private static final String RESOURCE = "dataset-registration-cases.properties";
    private static final String DEFAULT_FORMAT = "csv";
    private static final String DEFAULT_QUERY = "FROM %s | LIMIT 1";
    private static final String SETTINGS_PREFIX = "settings.";

    /** Every key shape the declaration recognises. A key matching none of these fails the load. */
    private static final Set<String> ATTRIBUTES = Set.of(
        "message",
        "absent",
        "emitter",
        "format",
        "outcome",
        "query",
        "resource",
        "resource_form",
        "blocked_by",
        "content",
        "content_charset"
    );

    /**
     * Expected columns are declared one per key rather than as a list.
     *
     * <p>A column name can contain whatever the bytes contain -- reading {@code a,b} with a non-comma
     * delimiter yields a single column called {@code a,b} -- so any separator this file chose could
     * also be inside a value. The same reason the dimension declaration names delimiter characters
     * rather than spelling them.
     */
    private static final String COLUMNS_PREFIX = "columns.";

    /** What counts as citing a defect: a filed issue a reader can open, not a bare number. */
    static final Pattern ISSUE_REFERENCE = Pattern.compile("elastic/[a-z0-9-]+#\\d+");

    /**
     * Where the registration is expected to fail.
     *
     * <p>Registration performs no I/O, so a setting it cannot decide about is not refused -- it is
     * accepted, and the failure arrives later when something finally opens the object. Those are two
     * different contracts with two different messages, and a suite that can only express the first
     * reports the second as a pass.
     */
    public enum Outcome {
        /** The PUT is refused, and the message is the assertion. */
        REFUSED,
        /**
         * The PUT is accepted and the QUERY fails.
         *
         * <p>A case of this kind pins both halves of the no-I/O promise at once: a PUT that starts
         * refusing it means registration acquired I/O, and the message after it is the contract for
         * what the reader says when it finally looks.
         */
        QUERY_FAILS,
        /**
         * The PUT is accepted, the query SUCCEEDS, and the result shows the setting took effect.
         *
         * <p>The outcome no status can detect. A setting that is accepted, plumbed to the reader and
         * then ignored returns rows exactly as one that worked does, so a case checking that rows came
         * back passes either way. What distinguishes them is the SHAPE of the result: bytes that parse
         * one way under the declared value and another way under the default.
         */
        QUERY_SUCCEEDS
    }

    /**
     * One registration that must be refused.
     *
     * @param name     the case name, so a failure says which refusal broke
     * @param settings  the dataset settings to register together, as the user would write them
     * @param message   a substring the refusal must contain
     * @param emitter   the symbol the message was read from, so the next reader can check it rather
     *                  than trusting that someone did
     * @param format    the format the dataset is registered as
     * @param outcome   whether the PUT is refused, or accepted with the query failing after it
     * @param query     the query to run for a {@code QUERY_FAILS} case, with {@code %s} for the dataset
     * @param resource  the fixture file to register, relative to the suite's fixture directory
     * @param rawPath   whether to register the resource as a bare filesystem path rather than a URI
     * @param absent    a substring the failure must NOT contain, or null
     * @param blockedBy the filed issue that stops this case passing today, or null when it passes
     * @param columns   for a {@code QUERY_SUCCEEDS} case, the column names the result must have
     * @param content   the bytes to write for this case, or null to use the suite's shared fixture
     * @param contentCharset the charset the content is WRITTEN in, which is the point of an encoding
     *                       case: bytes valid in the declared charset and invalid as UTF-8
     */
    public record Case(
        String name,
        Map<String, String> settings,
        String message,
        String emitter,
        String format,
        Outcome outcome,
        String query,
        String resource,
        boolean rawPath,
        String absent,
        String blockedBy,
        List<String> columns,
        String content,
        String contentCharset
    ) {
        public Case {
            settings = Map.copyOf(settings);
            columns = List.copyOf(columns);
        }

        /** Whether this case asserts behaviour the product does not have yet. */
        public boolean blocked() {
            return blockedBy != null;
        }
    }

    private final List<Case> cases;

    private RegistrationContract(List<Case> cases) {
        this.cases = List.copyOf(cases);
    }

    private static final RegistrationContract INSTANCE = load();

    public static RegistrationContract get() {
        return INSTANCE;
    }

    /** Every declared refusal, in declaration-name order so a shard slices the same list every run. */
    public List<Case> cases() {
        return cases;
    }

    private static RegistrationContract load() {
        Properties props = new Properties();
        try (InputStream in = RegistrationContract.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(
                    "[" + RESOURCE + "] is not on the classpath; the module reading it must depend on esql:qa:fixture-common"
                );
            }
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read [" + RESOURCE + "]", e);
        }
        return parse(props);
    }

    static RegistrationContract parse(Properties props) {
        Set<String> names = new TreeSet<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("case.") == false) {
                throw new IllegalStateException("unknown key [" + key + "] in [" + RESOURCE + "]; expected 'case.<name>.<attribute>'");
            }
            // Split from the RIGHT. A case name is a dotted path -- dataset.setting.skip_rows.negative --
            // so the first dot is part of the name, not the boundary. Splitting left put the whole
            // hierarchy into the attribute and every namespaced case read as one case called "dataset".
            String name = nameOf(key);
            String attribute = key.substring("case.".length() + name.length() + 1);
            // settings.<key> is the one attribute that takes a qualifier, because a case may pin more
            // than one setting: the interesting refusals at registration are combinations that are each
            // valid alone, and a contract of one setting per case cannot express one.
            if (attribute.startsWith(SETTINGS_PREFIX) == false
                && attribute.startsWith(COLUMNS_PREFIX) == false
                && ATTRIBUTES.contains(attribute) == false) {
                throw new IllegalStateException(
                    "case ["
                        + name
                        + "] declares unknown attribute ["
                        + attribute
                        + "]; expected settings.<key>, columns.<n> or one of "
                        + ATTRIBUTES
                );
            }
            names.add(name);
        }

        List<Case> parsed = new ArrayList<>();
        for (String name : names) {
            Map<String, String> settings = new LinkedHashMap<>();
            String prefix = "case." + name + "." + SETTINGS_PREFIX;
            for (String key : new TreeSet<>(props.stringPropertyNames())) {
                if (key.startsWith(prefix)) {
                    // NOT trimmed. A setting value can be whitespace and mean it -- a tab delimiter is
                    // the case that caught this, arriving as an empty string and silently reading the
                    // file on the comma default. Properties already strips what precedes the value, so
                    // what is left is what the case wrote.
                    settings.put(key.substring(prefix.length()), props.getProperty(key));
                }
            }
            if (settings.isEmpty()) {
                throw new IllegalStateException("case [" + name + "] declares no settings; there is nothing for it to register");
            }
            String format = props.getProperty("case." + name + ".format", DEFAULT_FORMAT).trim();
            String outcomeText = props.getProperty("case." + name + ".outcome", Outcome.REFUSED.name()).trim();
            Outcome outcome;
            try {
                outcome = Outcome.valueOf(outcomeText.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(
                    "case ["
                        + name
                        + "] declares unknown outcome ["
                        + outcomeText
                        + "]; expected one of "
                        + Arrays.toString(Outcome.values())
                );
            }
            // A failing case asserts a message; a succeeding one asserts a result shape. Requiring both
            // of every case would make one a formality somebody fills in, which is how a field stops
            // being read.
            boolean fails = outcome != Outcome.QUERY_SUCCEEDS;
            String message = fails ? required(props, name, "message") : "";
            // The emitter is required rather than helpful. A message with no symbol beside it cannot be
            // re-checked against the code without searching for the string, and a message that has
            // drifted is exactly the one the search will not find.
            String emitter = fails ? required(props, name, "emitter") : "";
            List<String> columns = List.of();
            if (outcome == Outcome.QUERY_SUCCEEDS) {
                // The whole assertion. Without it the case checks that rows came back, which is exactly
                // what a silently ignored setting also produces.
                columns = declaredColumns(props, name);
                if (columns.isEmpty()) {
                    throw new IllegalStateException("case [" + name + "] expects the query to succeed but names no columns");
                }
            } else if (declaredColumns(props, name).isEmpty() == false) {
                throw new IllegalStateException("case [" + name + "] does not expect a result, so its [columns] would never be compared");
            }
            String query = props.getProperty("case." + name + ".query", DEFAULT_QUERY).trim();
            // A refusal never runs a query, so declaring one says the case was written as the other kind
            // and half-converted -- which would read as covering the query path while never touching it.
            if (outcome == Outcome.REFUSED && props.getProperty("case." + name + ".query") != null) {
                throw new IllegalStateException("case [" + name + "] is refused at registration, so its [query] would never run");
            }
            // The default fixture is a well-formed file of the declared format. A query-failure case is
            // usually about bytes that do NOT match what the settings announce, so it names its own.
            // Bytes declared with the case. An effect case is only worth having when the data parses
            // differently under the setting than under the default, so the fixture is part of the case
            // rather than something shared -- and putting the discriminating bytes in the HEADER means
            // a mis-parse shows up as different column names rather than only different values.
            String content = props.getProperty("case." + name + ".content");
            // The charset the fixture is WRITTEN in, which is a different question from the encoding
            // the dataset declares. An encoding case is only meaningful when the two differ: bytes
            // that are valid in the declared charset and not valid UTF-8 are what separate a reader
            // honouring the setting from one that decoded with its default and substituted.
            String contentCharset = props.getProperty("case." + name + ".content_charset", "UTF-8").trim();
            if (content == null && props.getProperty("case." + name + ".content_charset") != null) {
                throw new IllegalStateException("case [" + name + "] declares a content_charset but no content to write in it");
            }
            String resource = props.getProperty("case." + name + ".resource", content != null ? name + "." + format : "simple." + format)
                .trim();
            String resourceForm = props.getProperty("case." + name + ".resource_form", "uri").trim();
            if (resourceForm.equals("uri") == false && resourceForm.equals("path") == false) {
                throw new IllegalStateException(
                    "case [" + name + "] declares resource_form [" + resourceForm + "]; expected [uri] or [path]"
                );
            }
            String absent = props.getProperty("case." + name + ".absent");
            absent = absent == null || absent.isBlank() ? null : absent.trim();
            // A case asserting behaviour the product does not have yet names the issue that will make it
            // pass. Without the citation nothing closes the exclusion when the defect is fixed, and the
            // case sits excluded long after the reason for it is gone -- which is a silent loss of the
            // coverage it was written to add.
            String blockedBy = props.getProperty("case." + name + ".blocked_by");
            if (blockedBy != null) {
                blockedBy = blockedBy.trim();
                if (ISSUE_REFERENCE.matcher(blockedBy).find() == false) {
                    throw new IllegalStateException(
                        "case ["
                            + name
                            + "] is blocked but cites no issue; write elastic/<repo>#<n> so the case can be "
                            + "un-blocked when the defect is fixed"
                    );
                }
            }
            parsed.add(
                new Case(
                    name,
                    settings,
                    message,
                    emitter,
                    format,
                    outcome,
                    query,
                    resource,
                    resourceForm.equals("path"),
                    absent,
                    blockedBy,
                    columns,
                    content,
                    contentCharset
                )
            );
        }
        if (parsed.isEmpty()) {
            throw new IllegalStateException("[" + RESOURCE + "] declares no cases; an empty contract asserts nothing and passes");
        }
        return new RegistrationContract(parsed);
    }

    /**
     * The case name inside a key, which is everything between {@code case.} and the attribute.
     *
     * <p>Names are dotted paths so the corpus can be read by subject rather than as one flat list:
     * {@code dataset.setting.skip_rows.negative} sits beside its siblings and apart from
     * {@code dataset.combination.probe_budget_exceeded}. The attribute is therefore the LAST segment,
     * except for {@code settings.<key>} and {@code columns.<n>}, where the qualifier is itself the last
     * segment and the attribute is the two together.
     */
    private static String nameOf(String key) {
        String rest = key.substring("case.".length());
        int last = rest.lastIndexOf('.');
        if (last < 0) {
            throw new IllegalStateException("malformed key [" + key + "]; expected 'case.<name>.<attribute>'");
        }
        String head = rest.substring(0, last);
        int previous = head.lastIndexOf('.');
        if (previous >= 0) {
            // The two attributes that take a qualifier: the key being registered and the column index
            // are themselves the last segment, so the attribute is the two segments together.
            String qualified = head.substring(previous + 1);
            if (qualified.equals("settings") || qualified.equals("columns")) {
                return head.substring(0, previous);
            }
        }
        return head;
    }

    /** The expected columns of a case, in the order their indices give. */
    private static List<String> declaredColumns(Properties props, String name) {
        String prefix = "case." + name + "." + COLUMNS_PREFIX;
        List<Integer> indices = new ArrayList<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                String tail = key.substring(prefix.length());
                try {
                    indices.add(Integer.parseInt(tail));
                } catch (NumberFormatException e) {
                    throw new IllegalStateException("case [" + name + "] declares [columns." + tail + "]; expected columns.<n>", e);
                }
            }
        }
        // Sorted by index, not by the string form, so columns.10 follows columns.9 rather than columns.1.
        indices.sort(Integer::compareTo);
        return indices.stream().map(i -> props.getProperty(prefix + i).trim()).toList();
    }

    private static String required(Properties props, String name, String attribute) {
        String value = props.getProperty("case." + name + "." + attribute);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("case [" + name + "] declares no [" + attribute + "]");
        }
        return value.trim();
    }
}
