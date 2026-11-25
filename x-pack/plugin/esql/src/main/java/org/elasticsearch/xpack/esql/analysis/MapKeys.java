/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.rules.ViewUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.FlattenedKeyEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
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
 *     {@code EVAL}, {@code LOOKUP JOIN}, {@code ENRICH}, {@code RENAME}. Renaming a map renames its child columns with it, and
 *     copying one ({@code EVAL x = attrs}) gives the copy the same children, hidden.
 *     Writing a value that is not a map replaces only the value at that name: a node can have a value and children, and the
 *     children that a source provided stay.</li>
 *     <li><b>{@code KEEP} and {@code DROP} take their patterns literally.</b> They select nodes by name and do not change the value
 *     of a map. {@code KEEP attrs} and {@code DROP attrs} name the one node {@code attrs}; {@code attrs*} names the tree. A child is
 *     readable until a {@code KEEP} or {@code DROP} excludes its name: {@code KEEP attrs} closes the map, {@code KEEP attrs*} keeps
 *     it open, and after {@code DROP attrs} the children that a source provided are still there, as a mapped field under that
 *     name is. Only a key that is read from a value needs that value to be in the output.</li>
 *     <li><b>Sources merge node by node.</b> In a union of indices or subqueries a map column holds whichever map a row has, and a
 *     child column resolves its type conflicts as any field does. Where maps are at two levels under a name ({@code attrs} in one
 *     index and {@code attrs.foo} in another), each row reads {@code attrs.foo.bar} from the nearest map it has: the result is the
 *     union of both. A row can only have both maps when the nearer one was written under the other later, and then the nearer one
 *     has replaced what was under its name (rule 3).</li>
 *     <li><b>{@code unmapped_fields} decides only what no map answers.</b> For a map from {@code FROM} the shards answer the name,
 *     so a key of one index sits beside the value another index maps or, under {@code load}, has in {@code _source}: that is one
 *     column. For a map with no index behind it the key is read from the value, and wins over {@code nullify} and {@code load}.</li>
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
    static LogicalPlan resolveKeys(LogicalPlan plan, boolean sourcesProvideChildren) {
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
            Expression read = read(name, input, plan.source(), sourcesProvideChildren);
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

    /**
     * Rule 1: what the name {@code name} reads from the output of {@code input}, or {@code null} if no map answers it.
     * <ul>
     *     <li>The child column of that name that the source of the map provides, where there is one: {@code FROM} resolves the
     *     names the query uses per shard, so the column holds a key of one index beside the mapped or loaded values of another.
     *     It is hidden, see {@link #hideKeys}.</li>
     *     <li>Otherwise the key in the value of the map. That is the only read there is for a map with no index behind it: the
     *     output of a view or of an aggregate. A lookup index and an enrich policy do have an index behind them, and should provide
     *     child columns as {@code FROM} does, so that a join loads the one key and not the whole map. They do not yet: today a key
     *     of their maps is read from the value.</li>
     * </ul>
     * Either way the map has to be in the output, and no projection may have excluded the name (rule 4).
     */
    private static Expression read(String name, LogicalPlan input, Source source, boolean sourcesProvideChildren) {
        Attribute hidden = null;
        List<Attribute> maps = new ArrayList<>();
        for (Attribute column : input.output()) {
            if (column.name().equals(name)) {
                if (column.synthetic() == false) {
                    // a column of that name: it is resolved like any column, not here
                    return null;
                }
                hidden = column;
            } else if (isMap(column.dataType()) && name.startsWith(column.name() + ".")) {
                maps.add(column);
            }
        }
        if (hidden != null && isOpen(input, hidden, name)) {
            // A child that the source provided is a node of its own: it stays when a value that is not a map is written at the
            // name above it (rule 3), and when the map is dropped while a pattern still keeps the child (rule 4)
            return hidden;
        }
        if (maps.isEmpty()) {
            // a key that is read from the value needs that value
            return null;
        }
        maps.removeIf(map -> isOpen(input, map, name) == false);
        if (maps.isEmpty()) {
            return null;
        }
        // A map that is a field of an index has its children from that index. Reading its key from the value instead means that
        // the name was not asked of the source, which loses the union of the shards' answers without failing.
        assert sourcesProvideChildren == false || maps.stream().noneMatch(map -> map instanceof FieldAttribute)
            : "the key [" + name + "] of a map that is a field of an index is read from its value: " + maps;
        return valueRead(name, maps, source);
    }

    /** Rule 5 for a read from the value: per row, the nearest of {@code maps} that the row has. */
    private static Expression valueRead(String name, List<Attribute> maps, Source source) {
        maps.sort(Comparator.comparing((Attribute map) -> map.name().length()).reversed());
        Attribute farthest = maps.get(maps.size() - 1);
        Expression read = keyRead(source, farthest, name.substring(farthest.name().length() + 1));
        for (int i = maps.size() - 2; i >= 0; i--) {
            Attribute map = maps.get(i);
            Expression nearer = keyRead(source, map, name.substring(map.name().length() + 1));
            read = new Case(source, new IsNotNull(source, map), List.of(nearer, read));
        }
        return read;
    }

    /**
     * Rule 1 and 5 at a union or a {@code FORK}: every branch provides the children that the scope reading the union names. A branch that reads an
     * index has them from its shards. A branch that only has the map provides the child by reading the key from the value, as a
     * hidden column on top of the branch, so that the union merges the children of its branches by name. Without it the rows of
     * such a branch would be null for a name that another branch answers.
     * <p>
     * A view is a source: its body is not looked into, and a projection inside it does not close the map for the query that reads
     * it. A subquery is not a source, so rule 4 applies across its body.
     *
     * @param names the names the scope that reads the union refers to, see {@link #namesAskedOfSources}
     */
    static MergePlan provideChildren(MergePlan union, Set<String> names) {
        if (names.isEmpty()) {
            return union;
        }
        List<LogicalPlan> branches = union.children();
        List<LogicalPlan> newBranches = new ArrayList<>(branches.size());
        boolean changed = false;
        for (int i = 0; i < branches.size(); i++) {
            LogicalPlan branch = branches.get(i);
            if (branch.resolved()) {
                boolean isView = union instanceof BranchPlan withViews && withViews.isViewBranch(i);
                List<Attribute> output = branch.output();
                Set<String> columns = new HashSet<>();
                for (Attribute attribute : output) {
                    columns.add(attribute.name());
                }
                List<Alias> children = new ArrayList<>();
                for (String name : names) {
                    if (columns.contains(name)) {
                        continue;
                    }
                    List<Attribute> maps = new ArrayList<>();
                    for (Attribute column : output) {
                        if (isMap(column.dataType()) && name.startsWith(column.name() + ".") && (isView || isOpen(branch, column, name))) {
                            maps.add(column);
                        }
                    }
                    if (maps.isEmpty() == false) {
                        children.add(new Alias(branch.source(), name, valueRead(name, maps, branch.source()), null, true));
                    }
                }
                if (children.isEmpty() == false) {
                    branch = new Eval(branch.source(), branch, children);
                    changed = true;
                }
            }
            newBranches.add(branch);
        }
        return changed ? (MergePlan) union.replaceChildren(newBranches) : union;
    }

    /**
     * The names that the sources of the scope {@code scope} are asked for: the dotted names it refers to outside view bodies, and
     * for each of them the name it had before every {@code RENAME} and every copy ({@code EVAL x = attrs}) of the scope. For {@code RENAME attrs AS a | WHERE a.foo == "x"}
     * the source answers {@code attrs.foo}, and the rename carries that child to {@code a.foo} (rule 3). This only adds names: a
     * name that is asked for without need is a hidden child that nothing reads.
     */
    public static Set<String> namesAskedOfSources(LogicalPlan scope) {
        Set<String> names = new LinkedHashSet<>();
        Map<String, String> renames = new LinkedHashMap<>();
        ViewUtils.forEachOutsideViews(scope, node -> {
            node.forEachExpression(UnresolvedAttribute.class, ua -> {
                if (isKeyName(ua)) {
                    names.add(ua.name());
                }
            });
            if (node instanceof Rename rename) {
                for (Alias renaming : rename.renamings()) {
                    if (renaming.child() instanceof NamedExpression before && before instanceof UnresolvedPattern == false) {
                        renames.put(renaming.name() + ".", before.name() + ".");
                    }
                }
            } else if (node instanceof Eval eval) {
                // a copy: EVAL x = attrs
                for (Alias field : eval.fields()) {
                    if (field.child() instanceof NamedExpression original && original instanceof UnresolvedPattern == false) {
                        renames.put(field.name() + ".", original.name() + ".");
                    }
                }
            } else if (node instanceof Project project) {
                for (NamedExpression projection : project.projections()) {
                    if (projection instanceof Alias alias && alias.child() instanceof Attribute before) {
                        renames.put(alias.name() + ".", before.name() + ".");
                    }
                }
            }
        });
        if (renames.isEmpty()) {
            return names;
        }
        Deque<String> pending = new ArrayDeque<>(names);
        while (pending.isEmpty() == false) {
            String name = pending.pop();
            for (Map.Entry<String, String> rename : renames.entrySet()) {
                if (name.startsWith(rename.getKey())) {
                    String before = rename.getValue() + name.substring(rename.getKey().length());
                    if (names.add(before)) {
                        pending.push(before);
                    }
                }
            }
        }
        return names;
    }

    /**
     * Rule 2 at the source: the key fields that {@code FROM} resolved for the names the query uses are children of their map, not
     * columns. They are marked synthetic, which the analyzer already has the machinery for: name resolution and wildcards do not
     * see a synthetic attribute, and the output of the query does not contain it. Only {@link #read} resolves a name to one, and a
     * command that takes it as a column makes a column of it. View bodies are left alone: the setting does not reach into a view.
     */
    static LogicalPlan hideKeys(LogicalPlan plan) {
        return ViewUtils.transformUpOutsideViews(withoutLookupKeysInViews(plan), node -> {
            if (node instanceof EsRelation == false) {
                return node;
            }
            EsRelation relation = (EsRelation) node;
            List<Attribute> output = relation.output();
            List<Attribute> hidden = new ArrayList<>(output.size());
            boolean changed = false;
            for (Attribute attribute : output) {
                if (attribute instanceof FieldAttribute key && key.synthetic() == false && FlattenedKeyEsField.isKey(key.field())) {
                    FieldAttribute child = new FieldAttribute(
                        key.source(),
                        key.parentName(),
                        key.qualifier(),
                        key.name(),
                        key.field(),
                        key.nullable(),
                        key.id(),
                        true
                    );
                    // A synthetic attribute reads the field its parent and field name spell; hide the key only if that is its name
                    if (child.fieldName().string().equals(key.name())) {
                        hidden.add(child);
                        changed = true;
                        continue;
                    }
                }
                hidden.add(attribute);
            }
            return changed ? relation.withAttributes(hidden) : relation;
        });
    }

    /**
     * A lookup index is resolved once for the query, with the names its caller asks of it. A join on the same index in the body of
     * a view must not see those key fields: the setting does not reach into a view, where a lookup index has no keys.
     */
    private static LogicalPlan withoutLookupKeysInViews(LogicalPlan plan) {
        return plan.transformUp(BranchPlan.class, union -> {
            List<LogicalPlan> branches = union.children();
            List<LogicalPlan> newBranches = new ArrayList<>(branches.size());
            boolean changed = false;
            for (int i = 0; i < branches.size(); i++) {
                LogicalPlan branch = branches.get(i);
                if (union.isViewBranch(i)) {
                    LogicalPlan withoutKeys = branch.transformUp(EsRelation.class, relation -> {
                        if (relation.indexMode() != IndexMode.LOOKUP) {
                            return relation;
                        }
                        List<Attribute> kept = new ArrayList<>(relation.output());
                        kept.removeIf(a -> a instanceof FieldAttribute field && FlattenedKeyEsField.isKey(field.field()));
                        return kept.size() == relation.output().size() ? relation : relation.withAttributes(kept);
                    });
                    changed |= withoutKeys != branch;
                    branch = withoutKeys;
                }
                newBranches.add(branch);
            }
            return changed ? union.replaceChildren(newBranches) : union;
        });
    }

    /**
     * Rule 1 for {@code ENRICH}: the enrich fields, and as hidden children the names in {@code names} that are under a map among
     * them. The enrich index answers those names as an index in {@code FROM} does. A field that {@code WITH} renames provides
     * none: the policy knows its children under the name the field has in the enrich index.
     */
    static List<NamedExpression> withChildrenOfEnrichFields(Source source, List<NamedExpression> enrichFields, Set<String> names) {
        if (names.isEmpty()) {
            return enrichFields;
        }
        Set<String> maps = new HashSet<>();
        Set<String> fields = new HashSet<>();
        for (NamedExpression field : enrichFields) {
            fields.add(field.name());
            if (field instanceof ReferenceAttribute && isMap(field.dataType())) {
                maps.add(field.name() + ".");
            }
        }
        if (maps.isEmpty()) {
            return enrichFields;
        }
        List<NamedExpression> withChildren = new ArrayList<>(enrichFields);
        for (String name : names.stream().sorted().toList()) {
            if (fields.contains(name) == false && prefixOf(name, maps) != null) {
                withChildren.add(new ReferenceAttribute(source, null, name, DataType.KEYWORD, Nullability.TRUE, null, true));
            }
        }
        return withChildren;
    }

    /**
     * Rule 4 for a {@code KEEP}: its patterns are taken literally for the hidden children too. A pattern does not see a hidden
     * attribute, so those that the patterns of the {@code KEEP} match are added to what it resolved to, and they stay hidden.
     */
    static List<NamedExpression> keepHiddenKeys(
        List<? extends NamedExpression> written,
        List<Attribute> input,
        List<NamedExpression> resolved
    ) {
        UnmappedFieldsPattern pattern = null;
        List<NamedExpression> result = null;
        Set<NameId> kept = null;
        for (Attribute attribute : input) {
            if (isHiddenKey(attribute) == false) {
                continue;
            }
            if (pattern == null) {
                pattern = UnmappedFieldsPattern.forKeep(written);
                kept = new HashSet<>();
                for (NamedExpression expression : resolved) {
                    if (expression.resolved()) {
                        kept.add(expression.id());
                    }
                }
            }
            if (kept.contains(attribute.id()) == false && pattern.matches(attribute.name())) {
                if (result == null) {
                    result = new ArrayList<>(resolved);
                }
                result.add(attribute);
                // A pattern that matches only hidden children matches something: it is not the unmatched pattern that the
                // resolution of the KEEP left behind for the verifier
                String name = attribute.name();
                result.removeIf(e -> e.resolved() == false && e instanceof UnresolvedAttribute && Regex.simpleMatch(e.name(), name));
            }
        }
        return result == null ? resolved : result;
    }

    private static boolean isHiddenKey(Attribute attribute) {
        return attribute.synthetic() && attribute.name().indexOf('.') > 0 && attribute.name().startsWith("$$") == false;
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
     * Rule 3 for a copy: {@code EVAL x = attrs} gives {@code x} the children of {@code attrs}. A copy of a map is the same map
     * under a second name, so what is under {@code attrs} is also under {@code x}: the columns and the hidden children alike.
     * The copies are hidden, so that copying a map adds one column to the output and not one per child.
     */
    static LogicalPlan copyChildren(LogicalPlan plan) {
        if (plan instanceof Eval == false || plan.resolved() == false) {
            return plan;
        }
        Eval eval = (Eval) plan;
        List<Attribute> input = eval.child().output();
        Set<String> written = new HashSet<>();
        for (Alias field : eval.fields()) {
            written.add(field.name());
        }
        List<Alias> children = new ArrayList<>();
        for (Alias field : eval.fields()) {
            if (field.child() instanceof Attribute == false || isMap(field.dataType()) == false) {
                continue;
            }
            String original = ((Attribute) field.child()).name() + ".";
            for (Attribute column : input) {
                if (column.name().startsWith(original)) {
                    String name = field.name() + "." + column.name().substring(original.length());
                    if (written.add(name)) {
                        children.add(new Alias(field.source(), name, column, null, true));
                    }
                }
            }
        }
        if (children.isEmpty()) {
            return plan;
        }
        List<Alias> fields = new ArrayList<>(eval.fields());
        fields.addAll(children);
        return new Eval(eval.source(), eval.child(), fields);
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
                    // a hidden child stays hidden under its new name
                    result.add(new Alias(column.source(), newName, column, null, column.synthetic()));
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
