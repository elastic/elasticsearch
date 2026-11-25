/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Map types: fields whose values hold named keys. {@code flattened} is the first one. This class is the whole contract for them and
 * the only place that applies it, behind {@code SET map_types=true}.
 *
 * <h2>The contract</h2>
 * One principle: <b>the columns of a query form a tree. A map is a node, and what is under its name belongs to it.</b>
 * <ol>
 *     <li><b>A map has children.</b> A child of the map {@code attrs} is anything named {@code attrs.<path>}: a column of that name,
 *     or a key in the value. Naming a child reads the column if there is one, and otherwise the key. The type of a key is a function
 *     of the map type and the path ({@code keyword} for any path of a {@code flattened}); a type with a schema answers from it.</li>
 *     <li><b>Reading a key does not create a column.</b> An expression reads the key in place. A command that takes a column
 *     ({@code KEEP attrs.foo}, {@code RENAME attrs.foo AS f}, {@code STATS … BY attrs.foo}) makes the key a child column.</li>
 *     <li><b>Children follow their map.</b> Writing a map at a name replaces everything under that name, whichever command writes it:
 *     {@code EVAL}, {@code LOOKUP JOIN}, {@code ENRICH}, {@code RENAME}. Renaming a map renames its child columns with it.
 *     Writing a value that is not a map replaces only that name.</li>
 *     <li><b>{@code KEEP} and {@code DROP} take their patterns literally.</b> They select columns by name and do not change the value
 *     of a map. A key is readable where its map is in the output and no {@code KEEP} or {@code DROP} since the map entered the query
 *     has excluded the key's name: {@code KEEP attrs} closes the map, {@code KEEP attrs*} keeps it open. Dropping a map takes its
 *     keys with it; its child columns that remain are plain columns.</li>
 *     <li><b>Sources merge node by node.</b> In a union of indices or subqueries a map column holds whichever map a row has, a child
 *     column resolves its type conflicts as any field does, and a name that is under maps at two levels reads the most specific
 *     one first.</li>
 *     <li><b>A key of a map in scope wins over {@code unmapped_fields}.</b> {@code nullify} and {@code load} decide only what no map
 *     and no column answers.</li>
 * </ol>
 * A new command gets all of this by declaring its output: rule 3 compares what a command puts out with what it took in, and
 * rules 1, 2 and 4 are applied where names are resolved. Do not add handling for maps to a command.
 *
 * <h2>Adding a map type</h2>
 * Extend {@link #isMap} and {@link #keyRead}. A type that cannot be read from a value (a {@code nested} field, whose children are
 * mapped leaves) returns no read: its children are then columns only, which rules 3 to 5 already cover.
 */
public final class MapKeys {

    private MapKeys() {}

    /** Whether values of {@code type} hold named keys. */
    public static boolean isMap(DataType type) {
        return type == DataType.FLATTENED;
    }

    /**
     * The read of the key {@code path} from the value of {@code map}. A key that a row does not hold is {@code null}, without a
     * warning: the name refers to a child of the map, and a child can be absent as a field of a document can.
     */
    static Expression keyRead(Source source, Attribute map, String path) {
        assert isMap(map.dataType());
        return new FieldExtract(source, map, Literal.keyword(source, path), true);
    }

    /**
     * Rules 1, 2 and 4 for one command: answers the dotted names that are still unresolved in {@code plan} with the keys of the maps
     * its input holds. A name that is a column has been resolved to it before this runs, so a column wins over a key.
     */
    static LogicalPlan resolveKeys(LogicalPlan plan) {
        if (plan instanceof UnaryPlan == false) {
            return plan;
        }
        UnaryPlan unary = (UnaryPlan) plan;
        LogicalPlan input = unary.child();
        if (input.resolved() == false) {
            return plan;
        }
        Set<String> names = new LinkedHashSet<>();
        plan.forEachExpression(UnresolvedAttribute.class, ua -> {
            if (isKeyName(ua)) {
                names.add(ua.name());
            }
        });
        if (names.isEmpty()) {
            return plan;
        }
        Map<String, Expression> reads = new LinkedHashMap<>();
        for (String name : names) {
            Expression read = read(name, input, plan.source());
            if (read != null) {
                reads.put(name, read);
            }
        }
        if (reads.isEmpty()) {
            return plan;
        }
        if (plan instanceof Filter || plan instanceof Eval || plan instanceof OrderBy) {
            // An expression reads the key in place
            return plan.transformExpressionsOnly(UnresolvedAttribute.class, ua -> isKeyName(ua) ? reads.getOrDefault(ua.name(), ua) : ua);
        }
        // A command that takes a column: the key becomes a child column below it, and the command resolves the name as any column
        List<Alias> columns = new ArrayList<>(reads.size());
        reads.forEach((name, read) -> columns.add(new Alias(plan.source(), name, read)));
        return unary.replaceChild(new Eval(plan.source(), input, columns));
    }

    private static boolean isKeyName(UnresolvedAttribute ua) {
        return ua instanceof UnresolvedPattern == false && ua.qualifier() == null && ua.name().indexOf('.') > 0;
    }

    private static Expression read(String name, LogicalPlan input, Source source) {
        List<Attribute> maps = new ArrayList<>();
        for (Attribute column : input.output()) {
            if (column.name().equals(name)) {
                return null;
            }
            if (isMap(column.dataType()) && name.startsWith(column.name() + ".") && isOpen(input, column, name)) {
                maps.add(column);
            }
        }
        if (maps.isEmpty()) {
            return null;
        }
        // rule 5: the most specific map first
        maps.sort(Comparator.comparing((Attribute map) -> map.name().length()).reversed());
        List<Expression> keyReads = new ArrayList<>(maps.size());
        for (Attribute map : maps) {
            keyReads.add(keyRead(source, map, name.substring(map.name().length() + 1)));
        }
        return keyReads.size() == 1 ? keyReads.get(0) : new Coalesce(source, keyReads.get(0), keyReads.subList(1, keyReads.size()));
    }

    /**
     * Rule 4: whether no {@code KEEP} or {@code DROP} between where {@code map} entered the query and {@code plan} has excluded
     * {@code name}. A rename or a copy of the map is the same map under another name, so the walk continues below it under the
     * name the key had there.
     */
    static boolean isOpen(LogicalPlan plan, Attribute map, String name) {
        LogicalPlan node = plan;
        while (true) {
            if (node instanceof Project project) {
                NamedExpression projection = byId(project.projections(), map.id());
                if (projection == null) {
                    return true;
                }
                if (project instanceof ResolvingProject resolving && resolving.admitsLateUnmappedField(name) == false) {
                    return false;
                }
                if (projection instanceof Alias alias) {
                    if (alias.child() instanceof Attribute == false) {
                        return true;
                    }
                    Attribute origin = (Attribute) alias.child();
                    name = origin.name() + name.substring(map.name().length());
                    map = origin;
                }
                node = project.child();
            } else if (node instanceof Eval eval) {
                NamedExpression definition = byId(eval.fields(), map.id());
                if (definition instanceof Alias alias) {
                    if (alias.child() instanceof Attribute == false) {
                        return true;
                    }
                    Attribute origin = (Attribute) alias.child();
                    name = origin.name() + name.substring(map.name().length());
                    map = origin;
                }
                node = eval.child();
            } else if (node instanceof UnaryPlan unary) {
                if (unary.child().outputSet().contains(map) == false) {
                    return true;
                }
                node = unary.child();
            } else if (node instanceof BinaryPlan binary && binary.left().outputSet().contains(map)) {
                node = binary.left();
            } else {
                return true;
            }
        }
    }

    private static NamedExpression byId(List<? extends NamedExpression> expressions, NameId id) {
        for (NamedExpression expression : expressions) {
            if (expression.id().equals(id)) {
                return expression;
            }
        }
        return null;
    }

    /**
     * Rule 3 for any command: if {@code plan} puts out a map that it did not take in, the columns under that name that it passed
     * through from its input belong to the map that was there before and are removed. The command is not asked what it does;
     * its output is compared with its input.
     */
    static LogicalPlan replaceSubtrees(LogicalPlan plan) {
        if (plan.resolved() == false || (plan instanceof UnaryPlan == false && plan instanceof BinaryPlan == false)) {
            return plan;
        }
        Set<NameId> inputIds = new HashSet<>();
        for (Attribute attribute : plan.children().get(0).output()) {
            inputIds.add(attribute.id());
        }
        List<Attribute> output = plan.output();
        List<String> written = new ArrayList<>();
        for (Attribute attribute : output) {
            if (isMap(attribute.dataType()) && inputIds.contains(attribute.id()) == false) {
                written.add(attribute.name() + ".");
            }
        }
        if (written.isEmpty()) {
            return plan;
        }
        List<Attribute> kept = new ArrayList<>(output.size());
        for (Attribute attribute : output) {
            boolean replaced = inputIds.contains(attribute.id()) && written.stream().anyMatch(attribute.name()::startsWith);
            if (replaced == false) {
                kept.add(attribute);
            }
        }
        return kept.size() == output.size() ? plan : new Project(plan.source(), plan, kept);
    }

    /**
     * Rule 3 for {@code RENAME}: the child columns of a renamed map are renamed with it, and replace what was under the new name.
     * {@code projections} are those of the rename, where an {@link Alias} of an attribute is one renaming.
     */
    static List<NamedExpression> renameChildren(List<NamedExpression> projections) {
        Map<String, String> renamedMaps = new LinkedHashMap<>();
        for (NamedExpression projection : projections) {
            if (projection instanceof Alias alias && alias.child() instanceof Attribute origin && isMap(origin.dataType())) {
                renamedMaps.put(origin.name() + ".", alias.name() + ".");
            }
        }
        if (renamedMaps.isEmpty()) {
            return projections;
        }
        List<NamedExpression> result = new ArrayList<>(projections.size());
        for (NamedExpression projection : projections) {
            if (projection instanceof Attribute column) {
                if (renamedMaps.containsValue(prefixOf(column.name(), renamedMaps.values()))) {
                    // under the new name of a map: it belonged to what the rename replaces
                    continue;
                }
                String oldPrefix = prefixOf(column.name(), renamedMaps.keySet());
                if (oldPrefix != null) {
                    String newName = renamedMaps.get(oldPrefix) + column.name().substring(oldPrefix.length());
                    result.add(new Alias(column.source(), newName, column));
                    continue;
                }
            }
            result.add(projection);
        }
        return result;
    }

    private static String prefixOf(String name, Iterable<String> prefixes) {
        for (String prefix : prefixes) {
            if (name.startsWith(prefix)) {
                return prefix;
            }
        }
        return null;
    }
}
