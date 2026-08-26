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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * The fixture matrix, read from {@code fixture-matrix.properties}.
 * <p>
 * The same file drives the build (via {@code fixture-matrix.gradle}, which decides what to
 * generate) and the spec suites (via this class, which decides where to look). That is the
 * point: the set a module generates and the set the suites expect used to be two independent
 * conventions, so a fixture could be generated and never read, or looked for and never
 * generated, without anything going red.
 */
public final class FixtureMatrix {

    /** The reserved layout whose file is named after the dataset rather than globbed. */
    public static final String STANDALONE = "standalone";

    private static final String RESOURCE = "fixture-matrix.properties";
    private static final FixtureMatrix INSTANCE = load();

    private final List<String> formats;
    private final Map<String, List<String>> datasetsByFormat;
    private final Map<String, String> restrictionReasons;
    private final List<String> baseline;
    private final List<Layout> layouts;
    private final int splitParts;

    /**
     * A fixture layout: the directory it occupies and how its files are addressed.
     *
     * @param name the layout name, which is also its template suffix ({@code multifile_split}
     *             is referenced as <code>{{employees_multifile_split}}</code>)
     * @param dir  the directory the generators write into and the suites glob
     * @param glob the pattern within that directory, or {@code null} for {@link #STANDALONE},
     *             whose file is named after the dataset
     */
    public record Layout(String name, String dir, String glob) {
        public boolean isStandalone() {
            return STANDALONE.equals(name);
        }

        /** The template suffix that selects this layout, e.g. {@code _multifile_split}. */
        public String suffix() {
            return "_" + name;
        }
    }

    public static FixtureMatrix get() {
        return INSTANCE;
    }

    private static FixtureMatrix load() {
        Properties props = new Properties();
        try (InputStream in = FixtureMatrix.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(
                    "fixture matrix resource ["
                        + RESOURCE
                        + "] is not on the classpath; the module reading it must depend on esql:qa:fixture-common"
                );
            }
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the fixture matrix [" + RESOURCE + "]", e);
        }
        return new FixtureMatrix(props);
    }

    private final Map<String, List<String>> specPatterns;
    private final Properties declaration;

    private FixtureMatrix(Properties props) {
        Map<String, List<String>> parsedSpecPatterns = new LinkedHashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("suite.") && key.endsWith(".specs")) {
                String token = key.substring("suite.".length(), key.length() - ".specs".length());
                List<String> patterns = Arrays.stream(props.getProperty(key).split(","))
                    .map(String::trim)
                    .filter(t -> t.isEmpty() == false)
                    .toList();
                parsedSpecPatterns.put(token, patterns);
            }
        }
        this.specPatterns = Map.copyOf(parsedSpecPatterns);
        this.declaration = props;
        this.formats = List.copyOf(splitList(required(props, "formats")));

        Map<String, List<String>> byFormat = new LinkedHashMap<>();
        for (String format : formats) {
            byFormat.put(format, new ArrayList<>());
        }
        Map<String, String> reasons = new LinkedHashMap<>();
        List<String> baselineDatasets = new ArrayList<>();

        List<String> datasetKeys = props.stringPropertyNames()
            .stream()
            // A dataset key is dataset.<name> and nothing further. Attribute keys carry another dot
            // segment (.reason, .write_dialect, ...); reading one as a dataset name parses its value as a
            // format list, which is how adding write_dialect produced a dataset called
            // "apps.write_dialect" restricted to a format called "none" -- in BOTH parsers of this file,
            // separately. Filter on the key's shape, not on a denylist of the suffixes we happen to know.
            .filter(k -> k.startsWith("dataset.") && k.indexOf('.', "dataset.".length()) < 0)
            .sorted()
            .toList();
        for (String key : datasetKeys) {
            String dataset = key.substring("dataset.".length());
            String value = required(props, key);
            List<String> declared;
            if ("*".equals(value)) {
                declared = formats;
                baselineDatasets.add(dataset);
            } else {
                // Opting out is a claim, and a claim needs a reason. Without this the matrix
                // cannot distinguish "no format can carry this" from "nobody got round to it".
                String reason = props.getProperty(key + ".reason");
                if (reason == null || reason.isBlank()) {
                    throw new IllegalStateException(
                        "dataset [" + dataset + "] is restricted to [" + value + "] but declares no [" + key + ".reason]"
                    );
                }
                reasons.put(dataset, reason.trim());
                declared = splitList(value);
                for (String format : declared) {
                    if (formats.contains(format) == false) {
                        throw new IllegalStateException(
                            "dataset [" + dataset + "] names unknown format [" + format + "]; declared formats are " + formats
                        );
                    }
                }
            }
            for (String format : declared) {
                byFormat.get(format).add(dataset);
            }
        }
        byFormat.replaceAll((format, datasets) -> List.copyOf(datasets));
        this.datasetsByFormat = Map.copyOf(byFormat);
        this.restrictionReasons = Map.copyOf(reasons);
        this.baseline = List.copyOf(baselineDatasets);

        List<Layout> declaredLayouts = new ArrayList<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("layout.") && key.endsWith(".dir")) {
                String name = key.substring("layout.".length(), key.length() - ".dir".length());
                String dir = required(props, key);
                String glob = STANDALONE.equals(name) ? null : props.getProperty("layout." + name + ".glob", "*").trim();
                declaredLayouts.add(new Layout(name, dir, glob));
            }
        }
        // Longest suffix first, so _multifile_split is never read as _multifile.
        declaredLayouts.sort(Comparator.comparingInt((Layout l) -> l.name().length()).reversed().thenComparing(Layout::name));
        this.layouts = List.copyOf(declaredLayouts);

        this.splitParts = Integer.parseInt(required(props, "layout.split.parts"));
    }

    /** The declared format axis. */
    public List<String> formats() {
        return formats;
    }

    /** The datasets the given format must materialise. */
    public List<String> datasetsFor(String format) {
        List<String> datasets = datasetsByFormat.get(format);
        if (datasets == null) {
            throw new IllegalArgumentException("unknown fixture format [" + format + "]; declared formats are " + formats);
        }
        return datasets;
    }

    /** The datasets declared for every format. */
    public List<String> baseline() {
        return baseline;
    }

    /** Whether the given dataset is declared for the given format. */
    public boolean declares(String format, String dataset) {
        return datasetsFor(format).contains(dataset);
    }

    /**
     * The declared format underlying a fixture file extension.
     * <p>
     * Compression is a separate dimension layered on top of a format, so the compressed suites
     * run with an extension like {@code csv.gz} while the matrix is indexed by {@code csv}. The
     * codec never changes which datasets exist -- every codec variant is generated from the same
     * declared set -- so stripping it is the whole of the mapping.
     */
    public static String baseFormat(String extension) {
        int dot = extension.indexOf('.');
        return dot < 0 ? extension : extension.substring(0, dot);
    }

    /**
     * Why the given dataset is restricted, or {@code null} if it is declared for every format.
     * A reason opens with {@code rule:} when no format could carry it and {@code gap:} when it
     * simply has not been propagated, so a caller can say which kind of absence it hit.
     */
    public String restrictionReason(String dataset) {
        return restrictionReasons.get(dataset);
    }

    /** Layouts, longest name first. */
    /**
     * The csv-spec patterns a suite loads, as declared in {@code suite.<token>.specs}.
     *
     * <p>Declared once because there are two consumers: the suite's {@code ParametersFactory}, and the
     * coverage gate that asks whether a declared cell has a reader. While the lists lived in the suites,
     * the gate could only approximate them by scanning directories, and a spec sitting in a scanned
     * directory that no suite loaded still counted as a consumer -- which reported the csv column covered
     * for hive_shadow while zero shadow cases ran on any CSV suite.
     */
    /**
     * The multi-value dialect the fixtures behind a template were WRITTEN in.
     *
     * <p>Resolved from the declaration rather than guessed: a standalone template is its own dataset, a
     * layout derived from a dataset inherits that dataset's dialect, and a layout assembled from its own
     * authored source files carries {@code none} -- those sources are bracket-free, which
     * checkFixtureDialect pins.
     *
     * <p>Per-source rather than per-suite on purpose. A single spec file can read one bracket-written
     * dataset and one bracket-free one, so a suite-wide setting would misread one of them; and injecting
     * {@code brackets} everywhere would retire the coverage of {@code none}, which is the default real
     * users get.
     */
    public String writeDialectForTemplate(String templateName) {
        Layout layout = layoutFor(templateName);
        String dataset;
        if (layout.isStandalone()) {
            dataset = templateName;
        } else {
            String derived = declaration.getProperty("layout." + layout.name() + ".derived_from");
            if (derived == null) {
                // Assembled from its own authored sources, which are bracket-free by construction.
                return "none";
            }
            dataset = derived.trim();
        }
        String declared = declaration.getProperty("dataset." + dataset + ".write_dialect");
        if (declared == null) {
            throw new IllegalStateException(
                "dataset ["
                    + dataset
                    + "] declares no write_dialect, so the read dialect for template ["
                    + templateName
                    + "] cannot be resolved"
            );
        }
        return declared.trim();
    }

    public List<String> specPatterns(String suiteToken) {
        List<String> patterns = specPatterns.get(suiteToken);
        if (patterns == null) {
            throw new IllegalStateException(
                "suite ["
                    + suiteToken
                    + "] declares no [suite."
                    + suiteToken
                    + ".specs]; its spec routing must be declared so the coverage gate reads the same list the suite loads"
            );
        }
        return patterns;
    }

    public List<Layout> layouts() {
        return layouts;
    }

    /** The layout named by a template's suffix, falling back to {@link #STANDALONE}. */
    public Layout layoutFor(String templateName) {
        for (Layout layout : layouts) {
            if (layout.isStandalone() == false && templateName.endsWith(layout.suffix())) {
                return layout;
            }
        }
        return layout(STANDALONE);
    }

    /** The layout with the given name. */
    public Layout layout(String name) {
        for (Layout layout : layouts) {
            if (layout.name().equals(name)) {
                return layout;
            }
        }
        throw new IllegalArgumentException("undeclared fixture layout [" + name + "]; declared layouts are " + layoutNames());
    }

    /** How many files a split layout produces. */
    public int splitParts() {
        return splitParts;
    }

    private List<String> layoutNames() {
        return layouts.stream().map(Layout::name).sorted().toList();
    }

    private static String required(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(String.format(Locale.ROOT, "the fixture matrix declares no [%s]", key));
        }
        return value.trim();
    }

    private static List<String> splitList(String value) {
        List<String> items = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty() == false) {
                items.add(trimmed);
            }
        }
        return items;
    }
}
