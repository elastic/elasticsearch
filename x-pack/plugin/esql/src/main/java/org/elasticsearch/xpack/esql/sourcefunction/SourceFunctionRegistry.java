/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * The source functions a source list may call, by name.
 * <p>
 * Built once at startup and identical on every node: nothing here consults cluster state, and a call's name
 * carries no privilege of its own. Built from a list of already-constructed functions, so a function that needs
 * collaborators is handed them where it is created and nothing shared has to know about them.
 * <p>
 * <b>It may legitimately be empty.</b> A node registering no function is a working state, not a half-built one,
 * and {@code resolve} answers for that case in its own words rather than listing an empty set.
 */
public final class SourceFunctionRegistry {

    private final Map<String, SourceFunction> byName;
    private final SortedSet<String> snapshotOnlyNames;

    /**
     * The shape a registered name may take. Narrower than the shape a call can be <i>written</i> in, and
     * deliberately: the token is {@code DEV_SOURCE_FUNCTION_NAME}, whose body without the trailing {@code (} is
     * {@code LETTER UNQUOTED_ID_BODY*} -- a letter, then letters, digits and underscores -- and the lexer is
     * declared {@code caseInsensitive=true}, so each of those letters matches in either case however the grammar's
     * own fragment is spelled. A written name is therefore matched whatever its case, which is what lets
     * {@code resolve} answer case-insensitively; what is registered must be lowercase, which is a separate check.
     * The shape itself is checked at registration because a name outside it is not refused anywhere
     * else — it simply never lexes as a call, so {@code FROM _x("a")} reads as an index pattern and a stray {@code (},
     * and the function sits in the registry permanently unreachable.
     */
    private static final Pattern LEXABLE_NAME = Pattern.compile("[a-z][a-z0-9_]*");

    /**
     * Everything a release build may call, and everything a snapshot build may call as well.
     * <p>
     * Two lists rather than a flag on the function, which is how {@code EsqlFunctionRegistry} splits the scalar
     * and aggregate functions: which build ships a thing is a property of the deployment, and a {@code
     * snapshotOnly()} marker on a pure function from invocation to relation would fuse that into the function
     * itself. A snapshot-only function is simply <b>absent</b> on a release build rather than present and
     * refused, so a query naming one is answered exactly as a query naming anything else that does not exist --
     * again the scalar registry's behaviour, and the reason there is no second error message here.
     */
    public SourceFunctionRegistry(List<SourceFunction> released, List<SourceFunction> snapshotOnly) {
        this(released, snapshotOnly, Build.current().isSnapshot());
    }

    /** Only the released functions, which is every caller that has no snapshot-only set to offer. */
    public SourceFunctionRegistry(List<SourceFunction> released) {
        this(released, List.of());
    }

    /**
     * The same, with the build's own answer passed in, so that what a release build does is reachable from a test
     * rather than only from a release build. That arm is the one nothing would otherwise exercise.
     */
    SourceFunctionRegistry(List<SourceFunction> released, List<SourceFunction> snapshotOnly, boolean snapshotBuild) {
        // Every name is checked on every build, registered or not: a function declared in the snapshot list with
        // a name a release build would reject is a mistake whichever build notices it, and leaving the check to
        // the build that happens to register it is how one survives to a release.
        Map<String, SourceFunction> declared = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (SourceFunction function : released) {
            declare(declared, function);
        }
        SortedSet<String> snapshotNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (SourceFunction function : snapshotOnly) {
            declare(declared, function);
            snapshotNames.add(function.name());
        }

        // TreeMap with a case-insensitive comparator: a user may write a name in any case, as they may for every
        // other name in the language, and the sorted key order is what an unknown-name error lists.
        Map<String, SourceFunction> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, SourceFunction> entry : declared.entrySet()) {
            if (snapshotBuild || snapshotNames.contains(entry.getKey()) == false) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        this.byName = map;
        this.snapshotOnlyNames = Collections.unmodifiableSortedSet(snapshotNames);
    }

    private static void declare(Map<String, SourceFunction> declared, SourceFunction function) {
        String name = function.name();
        if (name.equals(name.toLowerCase(Locale.ROOT)) == false) {
            throw new IllegalStateException("source function name [" + name + "] must be lowercase");
        }
        if (LEXABLE_NAME.matcher(name).matches() == false) {
            throw new IllegalStateException(
                "source function name [" + name + "] must start with a letter and hold only letters, digits and underscores"
            );
        }
        SourceFunction previous = declared.put(name, function);
        if (previous != null) {
            throw new IllegalStateException("duplicate source function [" + name + "]");
        }
    }

    /**
     * Whether this name is one only a snapshot build registers. Answers about the declaration rather than about
     * this registry's contents, so it is the same answer on either build -- which is what a caller reporting why
     * a name is unavailable needs.
     */
    public boolean isSnapshotOnly(String name) {
        return snapshotOnlyNames.contains(name);
    }

    /**
     * The function the given name resolves to, case-insensitively.
     * <p>
     * An unknown name is answered the way an unknown scalar function is — {@code UnresolvedFunction#missing} offers
     * the close names and nothing else — because a user who misspells one has the same question either way. The
     * registry here is small enough to list in full when nothing is close, which a registry of hundreds is not.
     *
     * @throws ParsingException if no function has that name
     */
    public SourceFunction resolve(Source source, String name) {
        SourceFunction function = byName.get(name);
        if (function == null) {
            // Its own arm when nothing is registered: "expected one of []" reads as a bug in the engine rather
            // than as the answer to what the user asked, and on a build with no function registered it is the
            // only message this method can produce.
            if (byName.isEmpty()) {
                throw new ParsingException(source, "no source functions are registered, so [{}] cannot be used here", name);
            }
            // Lowercased for the comparison only: the keys are lowercase by registration and the distance is
            // measured character by character, so FROM LETRERS(..) would otherwise be close to nothing at all.
            // The name is still reported as the user typed it.
            List<String> similar = StringUtils.findSimilar(name.toLowerCase(Locale.ROOT), byName.keySet());
            if (similar.isEmpty() == false) {
                throw new ParsingException(
                    source,
                    "unknown source function [{}], did you mean {}?",
                    name,
                    similar.size() == 1 ? "[" + similar.get(0) + "]" : "any of " + similar
                );
            }
            throw new ParsingException(
                source,
                "unknown source function [{}], expected one of [{}]",
                name,
                String.join(", ", byName.keySet())
            );
        }
        return function;
    }
}
