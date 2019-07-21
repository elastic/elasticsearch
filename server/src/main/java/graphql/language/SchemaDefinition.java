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
import static graphql.language.NodeUtil.directivesByName;

@PublicApi
public class SchemaDefinition extends AbstractNode<SchemaDefinition> implements SDLDefinition<SchemaDefinition> {

    private final List<Directive> directives;
    private final List<OperationTypeDefinition> operationTypeDefinitions;

    public static final String CHILD_DIRECTIVES = "directives";
    public static final String CHILD_OPERATION_TYPE_DEFINITIONS = "operationTypeDefinitions";

    @Internal
    protected SchemaDefinition(List<Directive> directives,
                               List<OperationTypeDefinition> operationTypeDefinitions,
                               SourceLocation sourceLocation,
                               List<Comment> comments,
                               IgnoredChars ignoredChars,
                               Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.directives = directives;
        this.operationTypeDefinitions = operationTypeDefinitions;
    }

    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    public Map<String, Directive> getDirectivesByName() {
        return directivesByName(directives);
    }

    public Directive getDirective(String directiveName) {
        return getDirectivesByName().get(directiveName);
    }


    public List<OperationTypeDefinition> getOperationTypeDefinitions() {
        return new ArrayList<>(operationTypeDefinitions);
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.addAll(directives);
        result.addAll(operationTypeDefinitions);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_DIRECTIVES, directives)
                .children(CHILD_OPERATION_TYPE_DEFINITIONS, operationTypeDefinitions)
                .build();
    }

    @Override
    public SchemaDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .directives(newChildren.getChildren(CHILD_DIRECTIVES))
                .operationTypeDefinitions(newChildren.getChildren(CHILD_OPERATION_TYPE_DEFINITIONS))
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
        return true;
    }

    @Override
    public SchemaDefinition deepCopy() {
        return new SchemaDefinition(deepCopy(directives), deepCopy(operationTypeDefinitions), getSourceLocation(), getComments(),
                getIgnoredChars(), getAdditionalData());
    }

    @Override
    public String toString() {
        return "SchemaDefinition{" +
                "directives=" + directives +
                ", operationTypeDefinitions=" + operationTypeDefinitions +
                "}";
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitSchemaDefinition(this, context);
    }

    public SchemaDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static Builder newSchemaDefinition() {
        return new Builder();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private List<Directive> directives = new ArrayList<>();
        private List<OperationTypeDefinition> operationTypeDefinitions = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(SchemaDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.directives = existing.getDirectives();
            this.operationTypeDefinitions = existing.getOperationTypeDefinitions();
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

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
            return this;
        }

        public Builder directive(Directive directive) {
            this.directives.add(directive);
            return this;
        }

        public Builder operationTypeDefinitions(List<OperationTypeDefinition> operationTypeDefinitions) {
            this.operationTypeDefinitions = operationTypeDefinitions;
            return this;
        }

        public Builder operationTypeDefinition(OperationTypeDefinition operationTypeDefinitions) {
            this.operationTypeDefinitions.add(operationTypeDefinitions);
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

        public SchemaDefinition build() {
            return new SchemaDefinition(directives,
                    operationTypeDefinitions,
                    sourceLocation,
                    comments,
                    ignoredChars,
                    additionalData);
        }
    }
}
