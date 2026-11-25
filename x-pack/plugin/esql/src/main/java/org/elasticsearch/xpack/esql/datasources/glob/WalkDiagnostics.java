/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Snapshot of a partition walk at the moment it stopped. Attached to listing-limit errors so the
 * user sees which keys had no filter and why the walk did not narrow. Never changes who fails versus
 * who falls back to a flat listing.
 */
record WalkDiagnostics(
    @Nullable StopReason stopReason,
    int listingsIssued,
    int maxDirectoryListings,
    boolean atDirectoryCeiling,
    int walkLevel,
    @Nullable String stoppedAtKey,
    List<String> pendingKeys,
    List<String> unfilteredKeys,
    List<String> unprunedHintedKeys,
    List<String> prunedColumns
) {
    enum StopReason {
        DECLINED_LAYOUT,
        LIST_CHILDREN_UNSUPPORTED,
        CEILING,
        UNHINTED_LEVEL,
        PEEK_BUDGET,
        STRAY,
        NOTHING_PRUNED,
        CHEAPER_TO_FINISH,
        ALWAYS_FLAT,
        FINISHED
    }

    /**
     * User-facing clause appended to a listing-limit error. Empty when the walk never started.
     */
    String describe() {
        List<String> clauses = new ArrayList<>();
        String stop = stopClause();
        if (stop.isEmpty() == false) {
            clauses.add(stop);
        }
        if (unfilteredKeys.isEmpty() == false) {
            clauses.add("No filter on [" + join(unfilteredKeys) + "]");
        }
        if (unprunedHintedKeys.isEmpty() == false) {
            clauses.add("[" + join(unprunedHintedKeys) + "] matched every folder");
        }
        if (pendingKeys.isEmpty() == false && stopReason != StopReason.FINISHED) {
            clauses.add("Filter on [" + join(pendingKeys) + "] did not bind to a folder");
        }
        if (prunedColumns.isEmpty() == false && stopReason == StopReason.FINISHED) {
            clauses.add("pruned [" + join(prunedColumns) + "]");
        }
        if (clauses.isEmpty()) {
            return "";
        }
        return String.join(". ", clauses) + ".";
    }

    String appendTo(String message) {
        String extra = describe();
        if (extra.isEmpty()) {
            return message;
        }
        return message + " " + extra;
    }

    private String stopClause() {
        String at = stoppedAtKey == null ? "" : " at [" + stoppedAtKey + "]";
        String listings = listingsIssued > 0 ? " after " + listingsIssued + " directory listings" : "";
        String ceiling = atDirectoryCeiling ? " (limit [esql.external.max_directory_listings]=" + maxDirectoryListings + ")" : "";
        StopReason reason = stopReason;
        if (reason == null) {
            if (listingsIssued <= 0) {
                return "";
            }
            return "The folder walk issued " + listingsIssued + " directory listings" + at;
        }
        switch (reason) {
            case CEILING, PEEK_BUDGET -> {
                return "The folder walk stopped" + listings + at + ceiling + nothingPrunedSuffix();
            }
            case UNHINTED_LEVEL -> {
                return "The folder walk stopped" + listings + at + nothingPrunedSuffix();
            }
            case LIST_CHILDREN_UNSUPPORTED -> {
                return "The folder walk could not list directories";
            }
            case DECLINED_LAYOUT -> {
                return "The folder walk could not align with the partition path";
            }
            case STRAY -> {
                return "The folder walk stopped because a folder did not match the expected layout";
            }
            case NOTHING_PRUNED, ALWAYS_FLAT, CHEAPER_TO_FINISH -> {
                return "The folder walk stopped" + listings + at + ceiling + "; nothing was pruned";
            }
            case FINISHED -> {
                if (listingsIssued <= 0) {
                    return "";
                }
                return "The folder walk issued " + listingsIssued + " directory listings" + at;
            }
            default -> throw new AssertionError("unexpected walk stop reason [" + reason + "]");
        }
    }

    private String nothingPrunedSuffix() {
        return prunedColumns.isEmpty() ? "; nothing was pruned" : "";
    }

    private static String join(List<String> keys) {
        return Strings.collectionToDelimitedString(keys, ", ");
    }
}
