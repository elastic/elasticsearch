package graphql.language;


import graphql.Internal;
import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static graphql.language.NodeChildrenContainer.newNodeChildrenContainer;
import static java.util.Collections.emptyMap;

@PublicApi
public class InputValueDefinition extends AbstractNode<InputValueDefinition> implements DirectivesContainer<InputValueDefinition>, NamedNode<InputValueDefinition> {
    private final String name;
    private final Type type;
    private final Value defaultValue;
    private final Description description;
    private final List<Directive> directives;

    public static final String CHILD_TYPE = "type";
    public static final String CHILD_DEFAULT_VALUE = "defaultValue";
    public static final String CHILD_DIRECTIVES = "directives";

    @Internal
    protected InputValueDefinition(String name,
                                   Type type,
                                   Value defaultValue,
                                   List<Directive> directives,
                                   Description description,
                                   SourceLocation sourceLocation,
                                   List<Comment> comments,
                                   IgnoredChars ignoredChars,
                                   Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
        this.directives = directives;
        this.description = description;

    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the input value
     * @param type of the input value
     */
    public InputValueDefinition(String name,
                                Type type) {
        this(name, type, null, new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());

    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name         of the input value
     * @param type         of the input value
     * @param defaultValue of the input value
     */

    public InputValueDefinition(String name,
                                Type type,
                                Value defaultValue) {
        this(name, type, defaultValue, new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());

    }

    public Type getType() {
        return type;
    }

    @Override
    public String getName() {
        return name;
    }

    public Description getDescription() {
        return description;
    }

    public Value getDefaultValue() {
        return defaultValue;
    }

    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.add(type);
        if (defaultValue != null) {
            result.add(defaultValue);
        }
        result.addAll(directives);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .child(CHILD_TYPE, type)
                .child(CHILD_DEFAULT_VALUE, defaultValue)
                .children(CHILD_DIRECTIVES, directives)
                .build();
    }

    @Override
    public InputValueDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .type(newChildren.getChildOrNull(CHILD_TYPE))
                .defaultValue(newChildren.getChildOrNull(CHILD_DEFAULT_VALUE))
                .directives(newChildren.getChildren(CHILD_DIRECTIVES))

        );
    }

    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InputValueDefinition that = (InputValueDefinition) o;

        return NodeUtil.isEqualTo(this.name, that.name);
    }

    @Override
    public InputValueDefinition deepCopy() {
        return new InputValueDefinition(name,
                deepCopy(type),
                deepCopy(defaultValue),
                deepCopy(directives),
                description,
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "InputValueDefinition{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", defaultValue=" + defaultValue +
                ", directives=" + directives +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitInputValueDefinition(this, context);
    }

    public static Builder newInputValueDefinition() {
        return new Builder();
    }

    public InputValueDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private Type type;
        private Value defaultValue;
        private Description description;
        private List<Directive> directives = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(InputValueDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.type = existing.getType();
            this.defaultValue = existing.getDefaultValue();
            this.description = existing.getDescription();
            this.directives = existing.getDirectives();
            this.additionalData = existing.getAdditionalData();
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

        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder defaultValue(Value defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder description(Description description) {
            this.description = description;
            return this;
        }

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
            return this;
        }

        public Builder directive(Directive directive) {
            this.directives.add(directive);
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


        public InputValueDefinition build() {
            return new InputValueDefinition(name,
                    type,
                    defaultValue,
                    directives,
                    description,
                    sourceLocation,
                    comments,
                    ignoredChars, additionalData);
        }
    }
}
