package graphql.analysis;

import graphql.Internal;
import graphql.execution.ConditionalNodes;
import graphql.execution.ValuesResolver;
import graphql.introspection.Introspection;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Node;
import graphql.language.NodeVisitorStub;
import graphql.language.TypeName;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLUnmodifiedType;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.Map;

import static graphql.Assert.assertNotNull;
import static graphql.schema.GraphQLTypeUtil.unwrapAll;
import static graphql.util.TraverserContext.Phase.LEAVE;

/**
 * Internally used node visitor which delegates to a {@link QueryVisitor} with type
 * information about the visited field.
 */
@Internal
public class NodeVisitorWithTypeTracking extends NodeVisitorStub {


    private final QueryVisitor preOrderCallback;
    private final QueryVisitor postOrderCallback;
    private final Map<String, Object> variables;
    private final GraphQLSchema schema;
    private final Map<String, FragmentDefinition> fragmentsByName;

    private final ConditionalNodes conditionalNodes = new ConditionalNodes();
    private final ValuesResolver valuesResolver = new ValuesResolver();


    public NodeVisitorWithTypeTracking(QueryVisitor preOrderCallback, QueryVisitor postOrderCallback, Map<String, Object> variables, GraphQLSchema schema, Map<String, FragmentDefinition> fragmentsByName) {
        this.preOrderCallback = preOrderCallback;
        this.postOrderCallback = postOrderCallback;
        this.variables = variables;
        this.schema = schema;
        this.fragmentsByName = fragmentsByName;
    }

    @Override
    public TraversalControl visitInlineFragment(InlineFragment inlineFragment, TraverserContext<Node> context) {
        if (!conditionalNodes.shouldInclude(variables, inlineFragment.getDirectives())) {
            return TraversalControl.ABORT;
        }

        QueryVisitorInlineFragmentEnvironment inlineFragmentEnvironment = new QueryVisitorInlineFragmentEnvironmentImpl(inlineFragment, context);

        if (context.getPhase() == LEAVE) {
            postOrderCallback.visitInlineFragment(inlineFragmentEnvironment);
            return TraversalControl.CONTINUE;
        }

        preOrderCallback.visitInlineFragment(inlineFragmentEnvironment);

        // inline fragments are allowed not have type conditions, if so the parent type counts
        QueryTraversalContext parentEnv = context.getVarFromParents(QueryTraversalContext.class);

        GraphQLCompositeType fragmentCondition;
        if (inlineFragment.getTypeCondition() != null) {
            TypeName typeCondition = inlineFragment.getTypeCondition();
            fragmentCondition = (GraphQLCompositeType) schema.getType(typeCondition.getName());
        } else {
            fragmentCondition = parentEnv.getUnwrappedOutputType();
        }
        // for unions we only have other fragments inside
        context.setVar(QueryTraversalContext.class, new QueryTraversalContext(fragmentCondition, parentEnv.getEnvironment(), inlineFragment));
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitFragmentDefinition(FragmentDefinition node, TraverserContext<Node> context) {
        if (!conditionalNodes.shouldInclude(variables, node.getDirectives())) {
            return TraversalControl.ABORT;
        }

        QueryVisitorFragmentDefinitionEnvironment fragmentEnvironment = new QueryVisitorFragmentDefinitionEnvironmentImpl(node, context);

        if (context.getPhase() == LEAVE) {
            postOrderCallback.visitFragmentDefinition(fragmentEnvironment);
            return TraversalControl.CONTINUE;
        }
        preOrderCallback.visitFragmentDefinition(fragmentEnvironment);

        QueryTraversalContext parentEnv = context.getVarFromParents(QueryTraversalContext.class);
        GraphQLCompositeType typeCondition = (GraphQLCompositeType) schema.getType(node.getTypeCondition().getName());
        context
                .setVar(QueryTraversalContext.class, new QueryTraversalContext(typeCondition, parentEnv.getEnvironment(), node));
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitFragmentSpread(FragmentSpread fragmentSpread, TraverserContext<Node> context) {
        if (!conditionalNodes.shouldInclude(variables, fragmentSpread.getDirectives())) {
            return TraversalControl.ABORT;
        }

        FragmentDefinition fragmentDefinition = fragmentsByName.get(fragmentSpread.getName());
        if (!conditionalNodes.shouldInclude(variables, fragmentDefinition.getDirectives())) {
            return TraversalControl.ABORT;
        }

        QueryVisitorFragmentSpreadEnvironment fragmentSpreadEnvironment = new QueryVisitorFragmentSpreadEnvironmentImpl(fragmentSpread, fragmentDefinition, context);
        if (context.getPhase() == LEAVE) {
            postOrderCallback.visitFragmentSpread(fragmentSpreadEnvironment);
            return TraversalControl.CONTINUE;
        }

        preOrderCallback.visitFragmentSpread(fragmentSpreadEnvironment);

        QueryTraversalContext parentEnv = context.getVarFromParents(QueryTraversalContext.class);

        GraphQLCompositeType typeCondition = (GraphQLCompositeType) schema.getType(fragmentDefinition.getTypeCondition().getName());
        assertNotNull(typeCondition, "Invalid type condition '%s' in fragment '%s'", fragmentDefinition.getTypeCondition().getName(),
                fragmentDefinition.getName());
        context
                .setVar(QueryTraversalContext.class, new QueryTraversalContext(typeCondition, parentEnv.getEnvironment(), fragmentDefinition));
        return TraversalControl.CONTINUE;
    }

    @Override
    public TraversalControl visitField(Field field, TraverserContext<Node> context) {
        QueryTraversalContext parentEnv = context.getVarFromParents(QueryTraversalContext.class);

        GraphQLFieldDefinition fieldDefinition = Introspection.getFieldDef(schema, (GraphQLCompositeType) unwrapAll(parentEnv.getOutputType()), field.getName());
        boolean isTypeNameIntrospectionField = fieldDefinition == Introspection.TypeNameMetaFieldDef;
        GraphQLFieldsContainer fieldsContainer = !isTypeNameIntrospectionField ? (GraphQLFieldsContainer) unwrapAll(parentEnv.getOutputType()) : null;
        GraphQLCodeRegistry codeRegistry = schema.getCodeRegistry();
        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(codeRegistry, fieldDefinition.getArguments(), field.getArguments(), variables);
        QueryVisitorFieldEnvironment environment = new QueryVisitorFieldEnvironmentImpl(isTypeNameIntrospectionField,
                field,
                fieldDefinition,
                parentEnv.getOutputType(),
                fieldsContainer,
                parentEnv.getEnvironment(),
                argumentValues,
                parentEnv.getSelectionSetContainer(),
                context);

        if (context.getPhase() == LEAVE) {
            postOrderCallback.visitField(environment);
            return TraversalControl.CONTINUE;
        }

        if (!conditionalNodes.shouldInclude(variables, field.getDirectives())) {
            return TraversalControl.ABORT;
        }

        TraversalControl traversalControl = preOrderCallback.visitFieldWithControl(environment);

        GraphQLUnmodifiedType unmodifiedType = unwrapAll(fieldDefinition.getType());
        QueryTraversalContext fieldEnv = (unmodifiedType instanceof GraphQLCompositeType)
                ? new QueryTraversalContext(fieldDefinition.getType(), environment, field)
                : new QueryTraversalContext(null, environment, field);// Terminal (scalar) node, EMPTY FRAME


        context.setVar(QueryTraversalContext.class, fieldEnv);
        return traversalControl;
    }

}
