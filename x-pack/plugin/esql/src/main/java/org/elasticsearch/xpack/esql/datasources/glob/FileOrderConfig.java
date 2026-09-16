/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * How a discovered file listing is ordered before schema resolution reads it.
 *
 * <p>First-file-wins takes the schema from {@code listing.path(0)}, so the listing order <em>is</em> the
 * donor. These two knobs are that order: what to sort by, and which direction. They are illegal on
 * {@code union_by_name} and {@code strict} — those strategies already merge or compare every file, and
 * file order must not change their column set. Registration and query {@code WITH} both reject the
 * knobs unless {@code schema_resolution} is {@code first_file_wins}.
 *
 * <p>When first-file-wins is on and the knobs are omitted, the listing is left in discovery order
 * ({@code list} + {@code asc}): a comma-separated resource keeps declaration order, a glob keeps the
 * provider's LIST order. Union-by-name and strict keep the historical name-ascending listing so
 * {@code SELECT *} column order and the strict reference file stay stable.
 *
 * <p>This is not query row order. Scan and {@code LIMIT} ignore it.
 */
public record FileOrderConfig(SortBy sortBy, Order order) {

    public static final String CONFIG_FILE_SORT_BY = "file_sort_by";
    public static final String CONFIG_FILE_ORDER = "file_order";

    /** The keys {@link #fromConfig} reads. */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_FILE_SORT_BY, CONFIG_FILE_ORDER);

    public static final FileOrderConfig DEFAULT = new FileOrderConfig(SortBy.LIST, Order.ASC);

    /** Historical listing order for union-by-name and strict. */
    public static final FileOrderConfig NAME_ASC = new FileOrderConfig(SortBy.NAME, Order.ASC);

    public enum SortBy {
        LIST,
        NAME,
        MTIME;

        /**
         * Case-insensitive parse of a {@code file_sort_by} value.
         *
         * @throws IllegalArgumentException if {@code value} is not a recognised sort
         */
        public static SortBy parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "list" -> LIST;
                case "name" -> NAME;
                case "mtime" -> MTIME;
                default -> throw new IllegalArgumentException(
                    "Unknown file_sort_by value [" + value + "]. Valid values are: list, name, mtime"
                );
            };
        }
    }

    public enum Order {
        ASC,
        DESC;

        /**
         * Case-insensitive parse of a {@code file_order} value.
         *
         * @throws IllegalArgumentException if {@code value} is not a recognised direction
         */
        public static Order parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "asc" -> ASC;
                case "desc" -> DESC;
                default -> throw new IllegalArgumentException("Unknown file_order value [" + value + "]. Valid values are: asc, desc");
            };
        }
    }

    public FileOrderConfig {
        if (sortBy == null) {
            throw new IllegalArgumentException("sortBy cannot be null");
        }
        if (order == null) {
            throw new IllegalArgumentException("order cannot be null");
        }
    }

    /**
     * Listing order for one expansion. First-file-wins uses the knobs (default {@code list}/{@code asc});
     * every other resolution, including an omitted {@code schema_resolution}, uses name-ascending.
     * Rejects the knobs when they are not legal, so a query {@code WITH} cannot silently ignore them.
     */
    public static FileOrderConfig forListing(@Nullable Map<String, Object> config) {
        validate(config);
        return firstFileWins(config) ? fromConfig(config) : NAME_ASC;
    }

    /**
     * Resolves the knobs with first-file-wins defaults: missing {@code file_sort_by} is {@code list}, missing
     * {@code file_order} is {@code asc}. Unknown values throw — unlike {@link ExclusionConfig#fromConfig} there
     * are no stored datasets to keep reading, so a bad value is never degraded.
     */
    public static FileOrderConfig fromConfig(@Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return DEFAULT;
        }
        Object sortByValue = config.get(CONFIG_FILE_SORT_BY);
        SortBy sortBy = sortByValue == null ? SortBy.LIST : SortBy.parse(sortByValue.toString());
        Object orderValue = config.get(CONFIG_FILE_ORDER);
        Order order = orderValue == null ? Order.ASC : Order.parse(orderValue.toString());
        return new FileOrderConfig(sortBy, order);
    }

    /**
     * Registration-time and query-time check. The knobs are only legal with
     * {@code schema_resolution = first_file_wins}; unknown values are named with the allowed lists.
     */
    public static void validate(@Nullable Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return;
        }
        boolean hasKnobs = config.get(CONFIG_FILE_SORT_BY) != null || config.get(CONFIG_FILE_ORDER) != null;
        if (hasKnobs == false) {
            return;
        }
        if (firstFileWins(config) == false) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_FILE_SORT_BY
                    + "] and ["
                    + CONFIG_FILE_ORDER
                    + "] are only valid with \"schema_resolution\": \"first_file_wins\""
            );
        }
        fromConfig(config);
    }

    private static boolean firstFileWins(@Nullable Map<String, Object> config) {
        if (config == null) {
            return false;
        }
        Object value = config.get(ExternalSourceResolver.CONFIG_SCHEMA_RESOLUTION);
        if (value == null) {
            return false;
        }
        try {
            return FormatReader.SchemaResolution.parse(value.toString()) == FormatReader.SchemaResolution.FIRST_FILE_WINS;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Orders {@code entries} in place. {@code list} + {@code asc} is a no-op (declaration / LIST order).
     * {@code mtime} ties break by path ascending regardless of {@code order}. Missing mtimes are epoch
     * (see {@link StorageEntry}), so they sort as oldest.
     */
    public void apply(List<StorageEntry> entries) {
        if (entries.size() < 2) {
            return;
        }
        switch (sortBy) {
            case LIST -> {
                if (order == Order.DESC) {
                    Collections.reverse(entries);
                }
            }
            case NAME -> entries.sort(nameOrder(order));
            case MTIME -> entries.sort(mtimeOrder(order));
        }
    }

    private static Comparator<StorageEntry> nameOrder(Order order) {
        Comparator<StorageEntry> byName = Comparator.comparing(e -> e.path().toString());
        return order == Order.DESC ? byName.reversed() : byName;
    }

    private static Comparator<StorageEntry> mtimeOrder(Order order) {
        Comparator<StorageEntry> byMtime = Comparator.comparingLong(e -> e.lastModified().toEpochMilli());
        Comparator<StorageEntry> byNameAsc = Comparator.comparing(e -> e.path().toString());
        return (order == Order.DESC ? byMtime.reversed() : byMtime).thenComparing(byNameAsc);
    }
}
