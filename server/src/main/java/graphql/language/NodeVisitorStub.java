package graphql.language;

import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

/**
 * Convenient implementation of {@link NodeVisitor} for easy subclassing methods handling different types of Nodes in one method.
 */
@PublicApi
public class NodeVisitorStub implements NodeVisitor {
    @Override
    public TraversalControl visitArgument(Argument node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitArrayValue(ArrayValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitBooleanValue(BooleanValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitDirective(Directive node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitDirectiveDefinition(DirectiveDefinition node, TraverserContext<Node> context) {
        return visitDefinition(node, context);
    }

    @Override
    public TraversalControl visitDirectiveLocation(DirectiveLocation node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitDocument(Document node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitEnumTypeDefinition(EnumTypeDefinition node, TraverserContext<Node> context) {
        return visitTypeDefinition(node, context);
    }

    @Override
    public TraversalControl visitEnumValue(EnumValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitEnumValueDefinition(EnumValueDefinition node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitField(Field node, TraverserContext<Node> context) {
        return visitSelection(node, context);
    }

    @Override
    public TraversalControl visitFieldDefinition(FieldDefinition node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitFloatValue(FloatValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitFragmentDefinition(FragmentDefinition node, TraverserContext<Node> context) {
        return visitDefinition(node, context);
    }

    @Override
    public TraversalControl visitFragmentSpread(FragmentSpread node, TraverserContext<Node> context) {
        return visitSelection(node, context);
    }

    @Override
    public TraversalControl visitInlineFragment(InlineFragment node, TraverserContext<Node> context) {
        return visitSelection(node, context);
    }

    @Override
    public TraversalControl visitInputObjectTypeDefinition(InputObjectTypeDefinition node, TraverserContext<Node> context) {
        return visitTypeDefinition(node, context);
    }

    @Override
    public TraversalControl visitInputValueDefinition(InputValueDefinition node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitIntValue(IntValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitInterfaceTypeDefinition(InterfaceTypeDefinition node, TraverserContext<Node> context) {
        return visitTypeDefinition(node, context);
    }

    @Override
    public TraversalControl visitListType(ListType node, TraverserContext<Node> context) {
        return visitType(node, context);
    }

    @Override
    public TraversalControl visitNonNullType(NonNullType node, TraverserContext<Node> context) {
        return visitType(node, context);
    }

    @Override
    public TraversalControl visitNullValue(NullValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitObjectField(ObjectField node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitObjectTypeDefinition(ObjectTypeDefinition node, TraverserContext<Node> context) {
        return visitTypeDefinition(node, context);
    }

    @Override
    public TraversalControl visitObjectValue(ObjectValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitOperationDefinition(OperationDefinition node, TraverserContext<Node> context) {
        return visitDefinition(node, context);
    }

    @Override
    public TraversalControl visitOperationTypeDefinition(OperationTypeDefinition node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitScalarTypeDefinition(ScalarTypeDefinition node, TraverserContext<Node> context) {
        return visitTypeDefinition(node, context);
    }

    @Override
    public TraversalControl visitSchemaDefinition(SchemaDefinition node, TraverserContext<Node> context) {
        return visitDefinition(node, context);
    }

    @Override
    public TraversalControl visitSelectionSet(SelectionSet node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitStringValue(StringValue node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }

    @Override
    public TraversalControl visitTypeName(TypeName node, TraverserContext<Node> context) {
        return visitType(node, context);
    }

    @Override
    public TraversalControl visitUnionTypeDefinition(UnionTypeDefinition node, TraverserContext<Node> context) {
        return visitTypeDefinition(node, context);
    }

    @Override
    public TraversalControl visitVariableDefinition(VariableDefinition node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    @Override
    public TraversalControl visitVariableReference(VariableReference node, TraverserContext<Node> context) {
        return visitValue(node, context);
    }


    protected TraversalControl visitValue(Value<?> node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    protected TraversalControl visitDefinition(Definition<?> node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    protected TraversalControl visitTypeDefinition(TypeDefinition<?> node, TraverserContext<Node> context) {
        return visitDefinition(node, context);
    }

    protected TraversalControl visitSelection(Selection<?> node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    protected TraversalControl visitType(Type<?> node, TraverserContext<Node> context) {
        return visitNode(node, context);
    }

    protected TraversalControl visitNode(Node node, TraverserContext<Node> context) {
        return TraversalControl.CONTINUE;
    }
}
