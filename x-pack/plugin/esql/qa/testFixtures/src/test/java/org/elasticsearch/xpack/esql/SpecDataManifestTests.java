/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader.Category;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Guards the spec-data manifest ({@code spec_data.yml}) against drift. For every csv-spec file it re-derives
 * the sources each query references — {@code FROM}/{@code TS} indices, {@code LOOKUP JOIN} targets,
 * {@code ENRICH} policies, and view references — and asserts the file's category actually provides them.
 * <p>
 * This is what keeps per-category data loading honest: adding {@code FROM some_new_index} to a test, or a
 * new csv-spec file, fails this test until the manifest's category (or file mapping) is updated. A prefix
 * wildcard (e.g. {@code emp*}) must live in a category that loads every index the pattern matches, so it
 * resolves the same as in production. A bare {@code FROM *} is category-scoped — it matches whatever the
 * category loads — so it is checked when the categorized suite runs, not statically here.
 */
public class SpecDataManifestTests extends ESTestCase {

    // FROM/TS only at a clause boundary: start of a line/query, after '|', after '(', or after ';' (a statement
    // separator, e.g. "SET ...; FROM ..."). The source list runs up to the next |, ( or ).
    private static final Pattern SRC = Pattern.compile("(?:^|\\||\\(|;|\\r|\\n)\\s*(?:FROM|TS)\\s+([^|()]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern JOIN = Pattern.compile("\\bLOOKUP\\s+JOIN\\s+\"?([A-Za-z0-9_.*\\-:]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern ENRICH = Pattern.compile("\\bENRICH\\s+(?:_[a-z]+:)?\"?([A-Za-z0-9_.*\\-:]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SRC_TAIL = Pattern.compile("\\bMETADATA\\b|\\bOPTIONS\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_.*\\-:@/]+");

    public void testEveryFileTargetIsProvidedByItsCategory() {
        Set<String> indices = CsvTestsDataLoader.CSV_DATASET.keySet();
        Set<String> views = CsvTestsDataLoader.VIEW_CONFIGS.keySet();
        List<String> problems = new ArrayList<>();

        for (String group : new TreeSet<>(CsvTestsDataLoader.FILE_CATEGORY.keySet())) {
            Category category = CsvTestsDataLoader.categoryFor(group);
            URL url = getClass().getResource("/" + group + ".csv-spec");
            assertNotNull("csv-spec file for manifest entry [" + group + "] not found on classpath", url);

            List<Object[]> specs;
            try {
                specs = SpecReader.readScriptSpec(List.of(url), CsvSpecReader::specParser);
            } catch (Exception e) {
                throw new AssertionError("Failed to parse [" + group + ".csv-spec]", e);
            }

            for (Object[] spec : specs) {
                String testName = (String) spec[2];
                CsvTestCase tc = (CsvTestCase) spec[4];
                Set<String> aliases = new HashSet<>();
                tc.datasetSources.forEach(ds -> aliases.add(ds.name()));

                for (Source source : sourcesOf(tc.query)) {
                    checkTarget(problems, group, testName, category, source, indices, views, aliases);
                }
                for (String policy : matches(ENRICH, tc.query)) {
                    if (category.enrich().contains(strip(policy)) == false) {
                        problems.add(
                            group
                                + " ["
                                + testName
                                + "]: ENRICH policy ["
                                + strip(policy)
                                + "] not in category ["
                                + category.name()
                                + "] enrich "
                                + category.enrich()
                        );
                    }
                }
            }
        }

        if (problems.isEmpty() == false) {
            fail("spec_data.yml categories do not cover all referenced data:\n  " + String.join("\n  ", problems));
        }
    }

    private record Source(String raw, boolean lookupJoin) {}

    private List<Source> sourcesOf(String query) {
        List<Source> out = new ArrayList<>();
        Matcher m = SRC.matcher(query);
        while (m.find()) {
            String list = SRC_TAIL.split(m.group(1))[0];
            for (String part : list.split(",")) {
                String t = part.trim();
                if (t.isEmpty() == false) {
                    out.add(new Source(t, false));
                }
            }
        }
        for (String t : matches(JOIN, query)) {
            out.add(new Source(t, true));
        }
        return out;
    }

    private void checkTarget(
        List<String> problems,
        String group,
        String testName,
        Category category,
        Source source,
        Set<String> indices,
        Set<String> views,
        Set<String> aliases
    ) {
        boolean excluded = source.raw().startsWith("-");
        String base = strip(source.raw());
        if (base.isEmpty() || IDENTIFIER.matcher(base).matches() == false) {
            return; // e.g. a "{{templated}}" external source or a parse artifact
        }
        if (base.equals("*")) {
            // A bare "FROM *" is category-scoped: it matches exactly the indices the category loads, so there is
            // nothing to require statically (result correctness is verified when the categorized suite runs).
            return;
        }
        boolean wildcard = base.indexOf('*') >= 0 || base.indexOf('?') >= 0;
        if (wildcard) {
            // A prefix wildcard (e.g. employees*) must resolve the same as in production, so the category has to
            // load every known index the pattern matches (and every matching view, if it loads views at all).
            if (excluded == false) {
                for (String index : indices) {
                    if (globMatches(base, index) && category.indices().contains(index) == false) {
                        problems.add(
                            group
                                + " ["
                                + testName
                                + "]: wildcard ["
                                + base
                                + "] matches index ["
                                + index
                                + "] not loaded by category ["
                                + category.name()
                                + "]"
                        );
                    }
                }
                if (category.loadsViews()) {
                    for (String view : views) {
                        if (globMatches(base, view) && category.views().contains(view) == false) {
                            problems.add(
                                group
                                    + " ["
                                    + testName
                                    + "]: wildcard ["
                                    + base
                                    + "] matches view ["
                                    + view
                                    + "] not loaded by category ["
                                    + category.name()
                                    + "]"
                            );
                        }
                    }
                }
            }
            return;
        }
        if (excluded || aliases.contains(base)) {
            return; // exclusions load nothing; external dataset aliases are provided outside the manifest
        }
        if (views.contains(base)) {
            if (category.views().contains(base) == false) {
                problems.add(
                    group + " [" + testName + "]: references view [" + base + "] but category [" + category.name() + "] does not load it"
                );
            }
            return;
        }
        if (indices.contains(base) == false) {
            return; // not a known index/view/alias (e.g. a ROW-defined name); nothing to load
        }
        if (category.indices().contains(base) == false) {
            problems.add(group + " [" + testName + "]: index [" + base + "] not in category [" + category.name() + "] indices");
        }
    }

    /** Matches an ES|QL index/view name pattern ({@code *} = any run, {@code ?} = one char) against a name. */
    private static boolean globMatches(String glob, String name) {
        StringBuilder regex = new StringBuilder(glob.length() + 8);
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            if (c == '*') {
                regex.append(".*");
            } else if (c == '?') {
                regex.append('.');
            } else {
                if ("\\.[]{}()+-^$|".indexOf(c) >= 0) {
                    regex.append('\\');
                }
                regex.append(c);
            }
        }
        return name.matches(regex.toString());
    }

    private static List<String> matches(Pattern p, String query) {
        List<String> out = new ArrayList<>();
        Matcher m = p.matcher(query);
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    /** Normalizes a raw source token: strip a leading '-' exclusion, a {@code ::selector} suffix, and a remote cluster prefix. */
    private static String strip(String raw) {
        String s = raw.trim();
        if (s.startsWith("-")) {
            s = s.substring(1);
        }
        s = s.replaceAll("\"", "");
        int sel = s.indexOf("::");
        if (sel >= 0) {
            s = s.substring(0, sel);
        }
        int colon = s.lastIndexOf(':');
        if (colon >= 0) {
            s = s.substring(colon + 1);
        }
        return s;
    }
}
