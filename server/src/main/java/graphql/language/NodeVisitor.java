package graphql.language;

import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

/**
 * Used by {@link NodeTraverser} to visit {@link Node}.
 */
@PublicApi
public interface NodeVisitor {
    TraversalControl visitArgument(Argument node, TraverserContext<Node> data);

    TraversalControl visitArrayValue(ArrayValue node, TraverserContext<Node> data);

    TraversalControl visitBooleanValue(BooleanValue node, TraverserContext<Node> data);

    TraversalControl visitDirective(Directive node, TraverserContext<Node> data);

    TraversalControl visitDirectiveDefinition(DirectiveDefinition node, TraverserContext<Node> data);

    TraversalControl visitDirectiveLocation(DirectiveLocation node, TraverserContext<Node> data);

    TraversalControl visitDocument(Document node, TraverserContext<Node> data);

    TraversalControl visitEnumTypeDefinition(EnumTypeDefinition node, TraverserContext<Node> data);

    TraversalControl visitEnumValue(EnumValue node, TraverserContext<Node> data);

    TraversalControl visitEnumValueDefinition(EnumValueDefinition node, TraverserContext<Node> data);

    TraversalControl visitField(Field node, TraverserContext<Node> data);

    TraversalControl visitFieldDefinition(FieldDefinition node, TraverserContext<Node> data);

    TraversalControl visitFloatValue(FloatValue node, TraverserContext<Node> data);

    TraversalControl visitFragmentDefinition(FragmentDefinition node, TraverserContext<Node> data);

    TraversalControl visitFragmentSpread(FragmentSpread node, TraverserContext<Node> data);

    TraversalControl visitInlineFragment(InlineFragment node, TraverserContext<Node> data);

    TraversalControl visitInputObjectTypeDefinition(InputObjectTypeDefinition node, TraverserContext<Node> data);

    TraversalControl visitInputValueDefinition(InputValueDefinition node, TraverserContext<Node> data);

    TraversalControl visitIntValue(IntValue node, TraverserContext<Node> data);

    TraversalControl visitInterfaceTypeDefinition(InterfaceTypeDefinition node, TraverserContext<Node> data);

    TraversalControl visitListType(ListType node, TraverserContext<Node> data);

    TraversalControl visitNonNullType(NonNullType node, TraverserContext<Node> data);

    TraversalControl visitNullValue(NullValue node, TraverserContext<Node> data);

    TraversalControl visitObjectField(ObjectField node, TraverserContext<Node> data);

    TraversalControl visitObjectTypeDefinition(ObjectTypeDefinition node, TraverserContext<Node> data);

    TraversalControl visitObjectValue(ObjectValue node, TraverserContext<Node> data);

    TraversalControl visitOperationDefinition(OperationDefinition node, TraverserContext<Node> data);

    TraversalControl visitOperationTypeDefinition(OperationTypeDefinition node, TraverserContext<Node> data);

    TraversalControl visitScalarTypeDefinition(ScalarTypeDefinition node, TraverserContext<Node> data);

    TraversalControl visitSchemaDefinition(SchemaDefinition node, TraverserContext<Node> data);

    TraversalControl visitSelectionSet(SelectionSet node, TraverserContext<Node> data);

    TraversalControl visitStringValue(StringValue node, TraverserContext<Node> data);

    TraversalControl visitTypeName(TypeName node, TraverserContext<Node> data);

    TraversalControl visitUnionTypeDefinition(UnionTypeDefinition node, TraverserContext<Node> data);

    TraversalControl visitVariableDefinition(VariableDefinition node, TraverserContext<Node> data);

    TraversalControl visitVariableReference(VariableReference node, TraverserContext<Node> data);
}
