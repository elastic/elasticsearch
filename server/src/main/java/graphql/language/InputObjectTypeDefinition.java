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

@PublicApi
public class InputObjectTypeDefinition extends AbstractNode<InputObjectTypeDefinition> implements TypeDefinition<InputObjectTypeDefinition>, DirectivesContainer<InputObjectTypeDefinition>, NamedNode<InputObjectTypeDefinition> {

    private final String name;
    private final Description description;
    private final List<Directive> directives;
    private final List<InputValueDefinition> inputValueDefinitions;

    public static final String CHILD_DIRECTIVES = "directives";
    public static final String CHILD_INPUT_VALUES_DEFINITIONS = "inputValueDefinitions";

    @Internal
    protected InputObjectTypeDefinition(String name,
                                        List<Directive> directives,
                                        List<InputValueDefinition> inputValueDefinitions,
                                        Description description,
                                        SourceLocation sourceLocation,
                                        List<Comment> comments,
                                        IgnoredChars ignoredChars,
                                        Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.description = description;
        this.directives = directives;
        this.inputValueDefinitions = inputValueDefinitions;
    }

    @Override
    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    public List<InputValueDefinition> getInputValueDefinitions() {
        return new ArrayList<>(inputValueDefinitions);
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
        result.addAll(directives);
        result.addAll(inputValueDefinitions);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_DIRECTIVES, directives)
                .children(CHILD_INPUT_VALUES_DEFINITIONS, inputValueDefinitions)
                .build();
    }

    @Override
    public InputObjectTypeDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .directives(newChildren.getChildren(CHILD_DIRECTIVES))
                .inputValueDefinitions(newChildren.getChildren(CHILD_INPUT_VALUES_DEFINITIONS))
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

        InputObjectTypeDefinition that = (InputObjectTypeDefinition) o;

        return NodeUtil.isEqualTo(this.name, that.name);
    }

    @Override
    public InputObjectTypeDefinition deepCopy() {
        return new InputObjectTypeDefinition(name,
                deepCopy(directives),
                deepCopy(inputValueDefinitions),
                description,
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "InputObjectTypeDefinition{" +
                "name='" + name + '\'' +
                ", directives=" + directives +
                ", inputValueDefinitions=" + inputValueDefinitions +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitInputObjectTypeDefinition(this, context);
    }


    public static Builder newInputObjectDefinition() {
        return new Builder();
    }

    public InputObjectTypeDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private Description description;
        private List<Directive> directives = new ArrayList<>();
        private List<InputValueDefinition> inputValueDefinitions = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(InputObjectTypeDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.description = existing.getDescription();
            this.directives = existing.getDirectives();
            this.inputValueDefinitions = existing.getInputValueDefinitions();
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

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
            return this;
        }

        public Builder directive(Directive directive) {
            this.directives.add(directive);
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


        public InputObjectTypeDefinition build() {
            return new InputObjectTypeDefinition(name,
                    directives,
                    inputValueDefinitions,
                    description,
                    sourceLocation,
                    comments,
                    ignoredChars,
                    additionalData);
        }
    }
}
