/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Detects Hive-style partition columns from file paths (e.g., {@code /year=2024/month=06/file.parquet}).
 * Parses key=value segments, validates consistency across all files, and infers types
 * using Spark-style rules extended for ES|QL: try Integer, Long, Unsigned Long, Double, Boolean,
 * fallback to keyword.
 */
public final class HivePartitionDetector implements PartitionDetector {

    public static final HivePartitionDetector INSTANCE = new HivePartitionDetector();

    /**
     * Sentinel directory name written by Hive for rows whose partition column is
     * NULL. When this token appears as a Hive-style key=value segment value, it must be surfaced as SQL
     * NULL rather than the literal string, otherwise {@code WHERE col IS NULL} silently misses rows and
     * {@code STATS BY col} buckets them under a phantom string key.
     */
    public static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    /**
     * Prefix applied to a partition column whose name collides with a dedicated metadata name.
     * Standard metadata ({@code _id}, {@code _index}, ...), the {@code _file.*} family, and
     * reader-synthesized channel names are reserved: a layout author cannot claim them, or
     * {@code METADATA _index} would silently return the partition value instead of its
     * spec-defined meaning (the dataset name). A directory like {@code /_index=foo/} surfaces as
     * {@code _partition._index} — the spec name keeps its meaning, the layout's value stays
     * queryable, and a notice on the caller's warning sink discloses each rename. Shared by every detector;
     * see {@link ReservedPartitionNames}.
     */
    public static final String RESERVED_RENAME_PREFIX = ReservedPartitionNames.RESERVED_RENAME_PREFIX;

    HivePartitionDetector() {}

    @Override
    public String name() {
        return "hive";
    }

    @Override
    public PartitionMetadata detect(List<StorageEntry> files, Consumer<String> warningSink) {
        Objects.requireNonNull(warningSink, "warningSink: a null sink would fall back to HeaderWarning off the request thread");
        if (files == null || files.isEmpty()) {
            return PartitionMetadata.EMPTY;
        }

        // One scratch map, cleared per file, decides the key set. The maps are not retained.
        LinkedHashMap<String, String> scratch = new LinkedHashMap<>();
        LinkedHashSet<String> referenceKeys = consistentKeys(files, scratch);
        if (referenceKeys == null) {
            return PartitionMetadata.EMPTY;
        }

        Map<String, String> surfacedNames = surfacedNames(referenceKeys, warningSink);
        if (surfacedNames == null) {
            return PartitionMetadata.EMPTY;
        }

        // Second scan fills one String[] per raw key, then casts into the columnar arrays.
        int fileCount = files.size();
        int cols = referenceKeys.size();
        String[] rawKeys = referenceKeys.toArray(String[]::new);
        String[][] rawValues = new String[cols][fileCount];
        for (int i = 0; i < fileCount; i++) {
            fillPartitions(files.get(i).path(), scratch);
            for (int c = 0; c < cols; c++) {
                rawValues[c][i] = scratch.get(rawKeys[c]);
            }
        }

        LinkedHashMap<String, DataType> partitionColumns = Maps.newLinkedHashMapWithExpectedSize(cols);
        Object[][] valuesByColumn = new Object[cols][];
        // One interner for this detect pass so sibling files share Integer/Long/keyword instances.
        CastInterner interner = new CastInterner();
        for (int c = 0; c < cols; c++) {
            String surface = surfacedNames.get(rawKeys[c]);
            DataType type = inferType(Arrays.asList(rawValues[c]));
            partitionColumns.put(surface, type);
            Object[] column = new Object[fileCount];
            String[] rawColumn = rawValues[c];
            for (int i = 0; i < fileCount; i++) {
                column[i] = castValue(rawColumn[i], type, interner);
            }
            valuesByColumn[c] = column;
        }
        return PartitionMetadata.columnar(partitionColumns, valuesByColumn, fileCount);
    }

    /**
     * Maps each detected partition key to the name it surfaces under. Non-reserved keys map to
     * themselves; keys colliding with a dedicated metadata name (see {@link #RESERVED_RENAME_PREFIX})
     * map to the prefixed form, with one notice on {@code warningSink} per rename. Returns
     * {@code null} — caller bails to {@link PartitionMetadata#EMPTY}, the detector's established
     * shape for unusable layouts — if a rename target collides with another detected key. That
     * branch is defensive: {@link #segmentKey} rejects a dotted key, so no parsed key
     * can currently equal a {@code _partition.}-prefixed name; the guard keeps the invariant
     * explicit should the segment grammar ever relax.
     */
    private static Map<String, String> surfacedNames(Set<String> referenceKeys, Consumer<String> warningSink) {
        Map<String, String> surfaced = Maps.newLinkedHashMapWithExpectedSize(referenceKeys.size());
        List<String> renamed = new ArrayList<>(0);
        for (String key : referenceKeys) {
            String surface = ReservedPartitionNames.surface(key);
            if (surface.equals(key) == false) {
                if (referenceKeys.contains(surface)) {
                    return null;
                }
                renamed.add(key);
            }
            surfaced.put(key, surface);
        }
        ReservedPartitionNames.warnRenamed(renamed, warningSink);
        return surfaced;
    }

    /**
     * Non-empty directory segments of a path string, object name dropped. Empty pieces from {@code //} are
     * skipped, then the last remaining segment — the object name — is dropped. A trailing slash does not put
     * that name back: {@code /data/year=2024/file.parquet/} and {@code /data/year=2024/file.parquet} both yield
     * {@code data}, {@code year=2024}. Hive detection and {@code hivePartitionValue} share this cut so a file
     * named {@code a=b.parquet} or {@code month=15} is not read as a partition folder.
     * {@link TemplatePartitionDetector#directorySegments} delegates here.
     */
    public static List<String> directorySegments(String path) {
        if (path == null || path.isEmpty()) {
            return List.of();
        }
        List<String> nonEmpty = new ArrayList<>();
        for (String segment : path.split("/")) {
            if (segment.isEmpty() == false) {
                nonEmpty.add(segment);
            }
        }
        if (nonEmpty.isEmpty()) {
            return List.of();
        }
        nonEmpty.remove(nonEmpty.size() - 1);
        return nonEmpty;
    }

    /**
     * Shared partition keys in first-file order, or {@code null} when a file binds nothing or the key sets
     * cannot be reconciled. A trailing {@code =} binds {@code ""}, so a base64 directory ({@code dXNlcjE=}) is
     * its own column and the key sets disagree. Those empty keys are dropped when they are missing from some
     * file; an empty key present on every file stays. A non-empty key missing from some file still voids the
     * detection. {@code scratch} is cleared per file and is not retained.
     */
    @Nullable
    private static LinkedHashSet<String> consistentKeys(List<StorageEntry> files, LinkedHashMap<String, String> scratch) {
        fillPartitions(files.get(0).path(), scratch);
        if (scratch.isEmpty()) {
            return null;
        }
        LinkedHashSet<String> reference = new LinkedHashSet<>(scratch.keySet());
        HashSet<String> shared = new HashSet<>(scratch.keySet());
        HashSet<String> nonEmpty = new HashSet<>();
        collectNonEmpty(scratch, nonEmpty);
        for (int i = 1; i < files.size(); i++) {
            fillPartitions(files.get(i).path(), scratch);
            if (scratch.isEmpty()) {
                return null;
            }
            // Compared to the first file's keys, not the shrinking intersection: a later file that still
            // carries every original key cannot remove anything the intersection still holds.
            if (reference.equals(scratch.keySet()) == false) {
                shared.retainAll(scratch.keySet());
            }
            collectNonEmpty(scratch, nonEmpty);
        }
        if (shared.isEmpty()) {
            return null;
        }
        for (String key : nonEmpty) {
            if (shared.contains(key) == false) {
                return null;
            }
        }
        reference.retainAll(shared);
        return reference;
    }

    /** Records keys whose value is not {@code ""} — those survive empty-token stripping. */
    private static void collectNonEmpty(Map<String, String> scratch, Set<String> nonEmpty) {
        for (Map.Entry<String, String> e : scratch.entrySet()) {
            // Null (the Hive default-partition sentinel) is not an empty token, so it is not dropped.
            if ("".equals(e.getValue()) == false) {
                nonEmpty.add(e.getKey());
            }
        }
    }

    /** Clears {@code into} and fills it with the first {@code key=value} binding of each directory segment. */
    private static void fillPartitions(StoragePath storagePath, LinkedHashMap<String, String> into) {
        into.clear();
        List<String> segments = directorySegments(storagePath.path());
        for (String segment : segments) {
            String key = segmentKey(segment);
            if (key == null || into.containsKey(key)) {
                continue;
            }
            into.put(key, segmentValue(segment));
        }
    }

    /**
     * The partition key a {@code key=value} path segment binds, or {@code null} when the segment is not
     * partition-shaped. Rejected: an empty segment, an empty key ({@code =value}, {@code ==}), a second
     * {@code =}, or a dot in the key. A dot or an empty tail in the value is a value ({@code price=1.5},
     * {@code k=}). Keys stay raw: {@code a%2Eb} is the column {@code a%2Eb}, not {@code a.b}.
     * The one segment grammar, shared with the listing walk via {@code PartitionValueMatcher}: pruning is sound
     * only while both layers parse identically.
     */
    static String segmentKey(String segment) {
        if (segment.isEmpty()) {
            return null;
        }
        int eqIdx = segment.indexOf('=');
        if (eqIdx <= 0) {
            return null;
        }
        if (segment.indexOf('=', eqIdx + 1) >= 0) {
            return null;
        }
        String key = segment.substring(0, eqIdx);
        if (key.indexOf('.') >= 0) {
            return null;
        }
        return key;
    }

    /** The decoded value of a {@code key=value} path segment ({@code null} for the NULL-partition sentinel); only
     * meaningful when {@link #segmentKey} accepted the segment. */
    static String segmentValue(String segment) {
        String value = decodePartitionValue(segment.substring(segment.indexOf('=') + 1));
        return HIVE_DEFAULT_PARTITION.equals(value) ? null : value;
    }

    /**
     * Decodes a partition folder value that a Hive-style writer percent-escaped. Hive escapes partition folder names
     * with {@code %XX} only and writes a literal {@code +} unescaped (it is not in the escape set). This is therefore
     * a plain UTF-8 percent-decode that keeps {@code +} literal, unlike {@code application/x-www-form-urlencoded}
     * decoding, which maps {@code +} to a space and so corrupts {@code a+b} to {@code "a b"} (a filter on the true
     * value then drops every row of that folder). This decodes {@code %XX} escapes as UTF-8 and keeps a literal
     * {@code +} as {@code +} by passing {@code plusAsSpace=false} explicitly, so the result does not depend on the
     * REST-only {@code es.rest.url_plus_as_space} setting; a malformed escape is left as the raw value rather than
     * failing the detection.
     *
     * <p>Shared by {@link TemplatePartitionDetector}, which decodes its own directory segments the same way.
     */
    static String decodePartitionValue(String value) {
        try {
            return RestUtils.decodeComponent(value, StandardCharsets.UTF_8, false);
        } catch (IllegalArgumentException e) {
            return value;
        }
    }

    public static DataType inferType(List<String> values) {
        DataType integralType = tryAllIntegral(values);
        if (integralType != null) {
            return integralType;
        }
        if (tryAllDouble(values)) {
            return DataType.DOUBLE;
        }
        if (tryAllBoolean(values)) {
            return DataType.BOOLEAN;
        }
        return DataType.KEYWORD;
    }

    private static DataType tryAllIntegral(List<String> values) {
        boolean needsLong = false;
        boolean needsUnsignedLong = false;
        boolean hasNegative = false;
        for (String v : values) {
            if (v == null) {
                continue;
            }
            try {
                Number n = StringUtils.parseIntegral(v);
                if (n instanceof BigInteger) {
                    needsUnsignedLong = true;
                } else {
                    if (n instanceof Long) {
                        needsLong = true;
                    }
                    if (n.longValue() < 0) {
                        hasNegative = true;
                    }
                }
            } catch (Exception e) {
                return null;
            }
        }
        if (needsUnsignedLong) {
            // A negative value and one above Long.MAX_VALUE have no exact common numeric type.
            return hasNegative ? DataType.KEYWORD : DataType.UNSIGNED_LONG;
        }
        return needsLong ? DataType.LONG : DataType.INTEGER;
    }

    private static boolean tryAllDouble(List<String> values) {
        for (String v : values) {
            if (v == null) {
                continue;
            }
            try {
                StringUtils.parseDouble(v);
            } catch (Exception e) {
                return false;
            }
            if (faithfulDoubleToken(v) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * A decimal whose fraction has a trailing zero past the first digit is not a double. {@code 1.10} and
     * {@code 1.1} would become one value, and {@code 2024.10} would surface as {@code 2024.1}. {@code 1.0} is the
     * canonical one-digit fraction and stays a double. Exponent forms ({@code 1e5}, {@code -0e0}) stay doubles.
     */
    private static boolean faithfulDoubleToken(String raw) {
        int exp = raw.indexOf('e');
        if (exp < 0) {
            exp = raw.indexOf('E');
        }
        if (exp >= 0) {
            return true;
        }
        int dot = raw.indexOf('.');
        if (dot < 0) {
            return true;
        }
        String fraction = raw.substring(dot + 1);
        return fraction.length() <= 1 || fraction.endsWith("0") == false;
    }

    private static boolean tryAllBoolean(List<String> values) {
        for (String v : values) {
            if (v == null) {
                continue;
            }
            if ("true".equalsIgnoreCase(v) == false && "false".equalsIgnoreCase(v) == false) {
                return false;
            }
        }
        return true;
    }

    static Object castValue(String value, DataType type) {
        return castValue(value, type, null);
    }

    /**
     * Casts one raw partition token. When {@code interner} is non-null (one detect pass), identical
     * {@link DataType#INTEGER}, {@link DataType#LONG}, and {@link DataType#KEYWORD} results share one
     * instance. {@code Integer.parseInt} already caches -128..127; the interner covers values outside
     * that range and every {@code Long}. Other types are left unshared. A null interner (filter
     * literals) allocates as before. Does not touch any map the caller already published.
     */
    static Object castValue(String value, DataType type, @Nullable CastInterner interner) {
        if (value == null) {
            return null;
        }
        Object cast;
        if (type == DataType.INTEGER) {
            cast = Integer.parseInt(value);
        } else if (type == DataType.LONG) {
            cast = Long.parseLong(value);
        } else if (type == DataType.UNSIGNED_LONG) {
            cast = DeclaredTypeCoercions.coerceToUnsignedLong(value);
        } else if (type == DataType.DOUBLE) {
            cast = Double.parseDouble(value);
        } else if (type == DataType.BOOLEAN) {
            // Match tryAllBoolean's case-insensitive inference: a folder typed BOOLEAN there (e.g. a standard
            // writer's flag=True/flag=False) must cast, so parse the same true/false-in-any-case token set.
            cast = DeclaredTypeCoercions.strictParseBoolean(value);
        } else {
            cast = value;
        }
        if (interner == null || cast == null) {
            return cast;
        }
        if (type == DataType.INTEGER || type == DataType.LONG || type == DataType.KEYWORD) {
            return interner.share(cast);
        }
        return cast;
    }

    /**
     * Query-scoped (one {@link #detect} call) identity map for repeated hive scalars.
     * Not retained after detect returns; the typed values stay reachable from the result maps.
     */
    static final class CastInterner {
        private final Map<Integer, Integer> integers = new HashMap<>();
        private final Map<Long, Long> longs = new HashMap<>();
        private final Map<String, String> strings = new HashMap<>();

        Object share(Object value) {
            if (value instanceof Integer i) {
                Integer existing = integers.putIfAbsent(i, i);
                return existing == null ? i : existing;
            }
            if (value instanceof Long l) {
                Long existing = longs.putIfAbsent(l, l);
                return existing == null ? l : existing;
            }
            if (value instanceof String s) {
                String existing = strings.putIfAbsent(s, s);
                return existing == null ? s : existing;
            }
            return value;
        }
    }
}
