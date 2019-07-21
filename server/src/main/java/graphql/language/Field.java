package graphql.language;


import graphql.Internal;
import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static java.util.Collections.emptyMap;

/*
 * This is provided to a DataFetcher, therefore it is a public API.
 * This might change in the future.
 */
@PublicApi
public class Field extends AbstractNode<Field> implements Selection<Field>, SelectionSetContainer<Field>, DirectivesContainer<Field>, NamedNode<Field> {

    private final String name;
    private final String alias;
    private final List<Argument> arguments;
    private final List<Directive> directives;
    private final SelectionSet selectionSet;

    public static final String CHILD_ARGUMENTS = "arguments";
    public static final String CHILD_DIRECTIVES = "directives";
    public static final String CHILD_SELECTION_SET = "selectionSet";


    @Internal
    protected Field(String name,
                    String alias,
                    List<Argument> arguments,
                    List<Directive> directives,
                    SelectionSet selectionSet,
                    SourceLocation sourceLocation,
                    List<Comment> comments,
                    IgnoredChars ignoredChars,
                    Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.alias = alias;
        this.arguments = arguments;
        this.directives = directives;
        this.selectionSet = selectionSet;
    }


    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the field
     */
    public Field(String name) {
        this(name, null, new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name      of the field
     * @param arguments to the field
     */
    public Field(String name, List<Argument> arguments) {
        this(name, null, arguments, new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name         of the field
     * @param arguments    to the field
     * @param selectionSet of the field
     */
    public Field(String name, List<Argument> arguments, SelectionSet selectionSet) {
        this(name, null, arguments, new ArrayList<>(), selectionSet, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name         of the field
     * @param selectionSet of the field
     */
    public Field(String name, SelectionSet selectionSet) {
        this(name, null, new ArrayList<>(), new ArrayList<>(), selectionSet, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.addAll(arguments);
        result.addAll(directives);
        if (selectionSet != null) {
            result.add(selectionSet);
        }
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return NodeChildrenContainer.newNodeChildrenContainer()
                .children(CHILD_ARGUMENTS, arguments)
                .children(CHILD_DIRECTIVES, directives)
                .child(CHILD_SELECTION_SET, selectionSet)
                .build();
    }

    @Override
    public Field withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder ->
                builder.arguments(newChildren.getChildren(CHILD_ARGUMENTS))
                        .directives(newChildren.getChildren(CHILD_DIRECTIVES))
                        .selectionSet(newChildren.getChildOrNull(CHILD_SELECTION_SET))
        );
    }

    @Override
    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public List<Argument> getArguments() {
        return Collections.unmodifiableList(arguments);
    }

    @Override
    public List<Directive> getDirectives() {
        return Collections.unmodifiableList(directives);
    }

    @Override
    public SelectionSet getSelectionSet() {
        return selectionSet;
    }


    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Field that = (Field) o;

        return NodeUtil.isEqualTo(this.name, that.name) && NodeUtil.isEqualTo(this.alias, that.alias);
    }

    @Override
    public Field deepCopy() {
        return new Field(name,
                alias,
                deepCopy(arguments),
                deepCopy(directives),
                deepCopy(selectionSet),
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData()
        );
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", alias='" + alias + '\'' +
                ", arguments=" + arguments +
                ", directives=" + directives +
                ", selectionSet=" + selectionSet +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitField(this, context);
    }

    public static Builder newField() {
        return new Builder();
    }

    public static Builder newField(String name) {
        return new Builder().name(name);
    }

    public static Builder newField(String name, SelectionSet selectionSet) {
        return new Builder().name(name).selectionSet(selectionSet);
    }

    public Field transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private String alias;
        private List<Argument> arguments = new ArrayList<>();
        private List<Directive> directives = new ArrayList<>();
        private SelectionSet selectionSet;
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(Field existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.alias = existing.getAlias();
            this.arguments = existing.getArguments();
            this.directives = existing.getDirectives();
            this.selectionSet = existing.getSelectionSet();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = new LinkedHashMap<>(existing.getAdditionalData());
        }


        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder comments(List<Comment> comments) {
            this.comments = comments;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder alias(String alias) {
            this.alias = alias;
            return this;
        }

        public Builder arguments(List<Argument> arguments) {
            this.arguments = arguments;
            return this;
        }

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
            return this;
        }

        public Builder selectionSet(SelectionSet selectionSet) {
            this.selectionSet = selectionSet;
            return this;
        }

        public Builder ignoredChars(IgnoredChars ignoredChars) {
            this.ignoredChars = ignoredChars;
            return this;
        }

        public Builder additionalData(Map<String, String> additionalData) {
            this.additionalData = assertNotNull(additionalData);
            return this;
        }

        public Builder additionalData(String key, String value) {
            this.additionalData.put(key, value);
            return this;
        }


        public Field build() {
            return new Field(name, alias, arguments, directives, selectionSet, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
