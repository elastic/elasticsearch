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
public class DirectiveDefinition extends AbstractNode<DirectiveDefinition> implements SDLDefinition<DirectiveDefinition>, NamedNode<DirectiveDefinition> {
    private final String name;
    private final Description description;
    private final List<InputValueDefinition> inputValueDefinitions;
    private final List<DirectiveLocation> directiveLocations;

    public static final String CHILD_INPUT_VALUE_DEFINITIONS = "inputValueDefinitions";
    public static final String CHILD_DIRECTIVE_LOCATION = "directiveLocation";

    @Internal
    protected DirectiveDefinition(String name,
                                  Description description,
                                  List<InputValueDefinition> inputValueDefinitions,
                                  List<DirectiveLocation> directiveLocations,
                                  SourceLocation sourceLocation,
                                  List<Comment> comments,
                                  IgnoredChars ignoredChars,
                                  Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.description = description;
        this.inputValueDefinitions = inputValueDefinitions;
        this.directiveLocations = directiveLocations;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the directive definition
     */
    public DirectiveDefinition(String name) {
        this(name, null, new ArrayList<>(), new ArrayList<>(), null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    @Override
    public String getName() {
        return name;
    }

    public Description getDescription() {
        return description;
    }

    public List<InputValueDefinition> getInputValueDefinitions() {
        return new ArrayList<>(inputValueDefinitions);
    }

    public List<DirectiveLocation> getDirectiveLocations() {
        return new ArrayList<>(directiveLocations);
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.addAll(inputValueDefinitions);
        result.addAll(directiveLocations);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_INPUT_VALUE_DEFINITIONS, inputValueDefinitions)
                .children(CHILD_DIRECTIVE_LOCATION, directiveLocations)
                .build();
    }

    @Override
    public DirectiveDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .inputValueDefinitions(newChildren.getChildren(CHILD_INPUT_VALUE_DEFINITIONS))
                .directiveLocations(newChildren.getChildren(CHILD_DIRECTIVE_LOCATION))
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

        DirectiveDefinition that = (DirectiveDefinition) o;

        return NodeUtil.isEqualTo(this.name, that.name);
    }

    @Override
    public DirectiveDefinition deepCopy() {
        return new DirectiveDefinition(name,
                description,
                deepCopy(inputValueDefinitions),
                deepCopy(directiveLocations),
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "DirectiveDefinition{" +
                "name='" + name + "'" +
                ", inputValueDefinitions=" + inputValueDefinitions +
                ", directiveLocations=" + directiveLocations +
                "}";
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitDirectiveDefinition(this, context);
    }

    public static Builder newDirectiveDefinition() {
        return new Builder();
    }

    public DirectiveDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private Description description;
        private List<InputValueDefinition> inputValueDefinitions = new ArrayList<>();
        private List<DirectiveLocation> directiveLocations = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(DirectiveDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.description = existing.getDescription();
            this.inputValueDefinitions = existing.getInputValueDefinitions();
            this.directiveLocations = existing.getDirectiveLocations();
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

        public Builder inputValueDefinitions(List<InputValueDefinition> inputValueDefinitions) {
            this.inputValueDefinitions = inputValueDefinitions;
            return this;
        }

        public Builder inputValueDefinition(InputValueDefinition inputValueDefinition) {
            this.inputValueDefinitions.add(inputValueDefinition);
            return this;
        }

        public Builder directiveLocations(List<DirectiveLocation> directiveLocations) {
            this.directiveLocations = directiveLocations;
            return this;
        }

        public Builder directiveLocation(DirectiveLocation directiveLocation) {
            this.directiveLocations.add(directiveLocation);
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


        public DirectiveDefinition build() {
            return new DirectiveDefinition(name, description, inputValueDefinitions, directiveLocations, sourceLocation, comments, ignoredChars, additionalData);
        }
    }
}
