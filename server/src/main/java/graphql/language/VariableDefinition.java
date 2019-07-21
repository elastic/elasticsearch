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
public class VariableDefinition extends AbstractNode<VariableDefinition> implements NamedNode<VariableDefinition> {

    private final String name;
    private final Type type;
    private final Value defaultValue;

    public static final String CHILD_TYPE = "type";
    public static final String CHILD_DEFAULT_VALUE = "defaultValue";

    @Internal
    protected VariableDefinition(String name,
                                 Type type,
                                 Value defaultValue,
                                 SourceLocation sourceLocation,
                                 List<Comment> comments,
                                 IgnoredChars ignoredChars,
                                 Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name         of the variable
     * @param type         of the variable
     * @param defaultValue of the variable
     */
    public VariableDefinition(String name,
                              Type type,
                              Value defaultValue) {
        this(name, type, defaultValue, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the variable
     * @param type of the variable
     */
    public VariableDefinition(String name,
                              Type type) {
        this(name, type, null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }


    public Value getDefaultValue() {
        return defaultValue;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.add(type);
        if (defaultValue != null) {
            result.add(defaultValue);
        }
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .child(CHILD_TYPE, type)
                .child(CHILD_DEFAULT_VALUE, defaultValue)
                .build();
    }

    @Override
    public VariableDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .type(newChildren.getChildOrNull(CHILD_TYPE))
                .defaultValue(newChildren.getChildOrNull(CHILD_DEFAULT_VALUE))
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

        VariableDefinition that = (VariableDefinition) o;

        return NodeUtil.isEqualTo(this.name, that.name);

    }

    @Override
    public VariableDefinition deepCopy() {
        return new VariableDefinition(name,
                deepCopy(type),
                deepCopy(defaultValue),
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "VariableDefinition{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", defaultValue=" + defaultValue +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitVariableDefinition(this, context);
    }


    public static Builder newVariableDefinition() {
        return new Builder();
    }

    public static Builder newVariableDefinition(String name) {
        return new Builder().name(name);
    }

    public static Builder newVariableDefinition(String name, Type type) {
        return new Builder().name(name).type(type);
    }

    public static Builder newVariableDefinition(String name, Type type, Value defaultValue) {
        return new Builder().name(name).type(type).defaultValue(defaultValue);
    }

    public VariableDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private String name;
        private List<Comment> comments = new ArrayList<>();
        private Type type;
        private Value defaultValue;
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(VariableDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.type = existing.getType();
            this.defaultValue = existing.getDefaultValue();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }

        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder comments(List<Comment> comments) {
            this.comments = comments;
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

        public VariableDefinition build() {
            return new VariableDefinition(
                    name,
                    type,
                    defaultValue,
                    sourceLocation,
                    comments,
                    ignoredChars,
                    additionalData);
        }
    }
}
