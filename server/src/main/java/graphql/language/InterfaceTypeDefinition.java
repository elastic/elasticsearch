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
public class InterfaceTypeDefinition extends AbstractNode<InterfaceTypeDefinition> implements TypeDefinition<InterfaceTypeDefinition>, DirectivesContainer<InterfaceTypeDefinition>, NamedNode<InterfaceTypeDefinition> {

    private final String name;
    private final Description description;
    private final List<FieldDefinition> definitions;
    private final List<Directive> directives;

    public static final String CHILD_DEFINITIONS = "definitions";
    public static final String CHILD_DIRECTIVES = "directives";

    @Internal
    protected InterfaceTypeDefinition(String name,
                                      List<FieldDefinition> definitions,
                                      List<Directive> directives,
                                      Description description,
                                      SourceLocation sourceLocation,
                                      List<Comment> comments,
                                      IgnoredChars ignoredChars,
                                      Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.definitions = definitions;
        this.directives = directives;
        this.description = description;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the interface
     */
    public InterfaceTypeDefinition(String name) {
        this(name, new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    public List<FieldDefinition> getFieldDefinitions() {
        return new ArrayList<>(definitions);
    }

    @Override
    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    @Override
    public String getName() {
        return name;
    }

    public Description getDescription() {
        return description;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.addAll(definitions);
        result.addAll(directives);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_DEFINITIONS, definitions)
                .children(CHILD_DIRECTIVES, directives)
                .build();
    }

    @Override
    public InterfaceTypeDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .definitions(newChildren.getChildren(CHILD_DEFINITIONS))
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

        InterfaceTypeDefinition that = (InterfaceTypeDefinition) o;

        return NodeUtil.isEqualTo(this.name, that.name);
    }

    @Override
    public InterfaceTypeDefinition deepCopy() {
        return new InterfaceTypeDefinition(name,
                deepCopy(definitions),
                deepCopy(directives),
                description,
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "InterfaceTypeDefinition{" +
                "name='" + name + '\'' +
                ", fieldDefinitions=" + definitions +
                ", directives=" + directives +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitInterfaceTypeDefinition(this, context);
    }


    public static Builder newInterfaceTypeDefinition() {
        return new Builder();
    }

    public InterfaceTypeDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private Description description;
        private List<FieldDefinition> definitions = new ArrayList<>();
        private List<Directive> directives = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }


        private Builder(InterfaceTypeDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.description = existing.getDescription();
            this.directives = existing.getDirectives();
            this.definitions = existing.getFieldDefinitions();
            this.ignoredChars = existing.getIgnoredChars();
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

        public Builder description(Description description) {
            this.description = description;
            return this;
        }

        public Builder definitions(List<FieldDefinition> definitions) {
            this.definitions = definitions;
            return this;
        }

        public Builder definition(FieldDefinition definition) {
            this.definitions.add(definition);
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


        public InterfaceTypeDefinition build() {
            return new InterfaceTypeDefinition(name,
                    definitions,
                    directives,
                    description,
                    sourceLocation,
                    comments,
                    ignoredChars,
                    additionalData);
        }
    }
}
