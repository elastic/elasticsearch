package graphql.validation;


import graphql.Assert;
import graphql.Internal;
import graphql.execution.TypeFromAST;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.Directive;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.InlineFragment;
import graphql.language.Node;
import graphql.language.ObjectField;
import graphql.language.OperationDefinition;
import graphql.language.SelectionSet;
import graphql.language.TypeName;
import graphql.language.VariableDefinition;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLNullableType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLUnionType;
import graphql.schema.GraphQLUnmodifiedType;
import graphql.schema.SchemaUtil;

import java.util.ArrayList;
import java.util.List;

import static graphql.introspection.Introspection.SchemaMetaFieldDef;
import static graphql.introspection.Introspection.TypeMetaFieldDef;
import static graphql.introspection.Introspection.TypeNameMetaFieldDef;
import static graphql.schema.GraphQLTypeUtil.isList;
import static graphql.schema.GraphQLTypeUtil.isNonNull;
import static graphql.schema.GraphQLTypeUtil.unwrapAll;
import static graphql.schema.GraphQLTypeUtil.unwrapOne;

@Internal
public class TraversalContext implements DocumentVisitor {
    final GraphQLSchema schema;
    final List<GraphQLOutputType> outputTypeStack = new ArrayList<>();
    final List<GraphQLCompositeType> parentTypeStack = new ArrayList<>();
    final List<GraphQLInputType> inputTypeStack = new ArrayList<>();
    final List<GraphQLFieldDefinition> fieldDefStack = new ArrayList<>();
    final List<String> nameStack = new ArrayList<>();
    GraphQLDirective directive;
    GraphQLArgument argument;


    public TraversalContext(GraphQLSchema graphQLSchema) {
        this.schema = graphQLSchema;
    }

    @Override
    public void enter(Node node, List<Node> path) {
        if (node instanceof OperationDefinition) {
            enterImpl((OperationDefinition) node);
        } else if (node instanceof SelectionSet) {
            enterImpl((SelectionSet) node);
        } else if (node instanceof Field) {
            enterImpl((Field) node);
        } else if (node instanceof Directive) {
            enterImpl((Directive) node);
        } else if (node instanceof InlineFragment) {
            enterImpl((InlineFragment) node);
        } else if (node instanceof FragmentDefinition) {
            enterImpl((FragmentDefinition) node);
        } else if (node instanceof VariableDefinition) {
            enterImpl((VariableDefinition) node);
        } else if (node instanceof Argument) {
            enterImpl((Argument) node);
        } else if (node instanceof ArrayValue) {
            enterImpl((ArrayValue) node);
        } else if (node instanceof ObjectField) {
            enterImpl((ObjectField) node);
        }
    }


    private void enterImpl(SelectionSet selectionSet) {
        GraphQLUnmodifiedType rawType = unwrapAll(getOutputType());
        GraphQLCompositeType parentType = null;
        if (rawType instanceof GraphQLCompositeType) {
            parentType = (GraphQLCompositeType) rawType;
        }
        addParentType(parentType);
    }

    private void enterImpl(Field field) {
        enterName(field.getName());
        GraphQLCompositeType parentType = getParentType();
        GraphQLFieldDefinition fieldDefinition = null;
        if (parentType != null) {
            fieldDefinition = getFieldDef(schema, parentType, field);
        }
        addFieldDef(fieldDefinition);
        addOutputType(fieldDefinition != null ? fieldDefinition.getType() : null);
    }

    private void enterImpl(Directive directive) {
        this.directive = schema.getDirective(directive.getName());
    }

    private void enterImpl(OperationDefinition operationDefinition) {
        if (operationDefinition.getOperation() == OperationDefinition.Operation.MUTATION) {
            addOutputType(schema.getMutationType());
        } else if (operationDefinition.getOperation() == OperationDefinition.Operation.QUERY) {
            addOutputType(schema.getQueryType());
        } else if (operationDefinition.getOperation() == OperationDefinition.Operation.SUBSCRIPTION) {
            addOutputType(schema.getSubscriptionType());
        } else {
            Assert.assertShouldNeverHappen();
        }
    }

    private void enterImpl(InlineFragment inlineFragment) {
        TypeName typeCondition = inlineFragment.getTypeCondition();
        GraphQLOutputType type;
        if (typeCondition != null) {
            GraphQLType typeConditionType = schema.getType(typeCondition.getName());
            if (typeConditionType instanceof GraphQLOutputType) {
                type = (GraphQLOutputType) typeConditionType;
            } else {
                type = null;
            }
        } else {
            type = getParentType();
        }
        addOutputType(type);
    }

    private void enterImpl(FragmentDefinition fragmentDefinition) {
        enterName(fragmentDefinition.getName());
        GraphQLType type = schema.getType(fragmentDefinition.getTypeCondition().getName());
        addOutputType(type instanceof GraphQLOutputType ? (GraphQLOutputType) type : null);
    }

    private void enterImpl(VariableDefinition variableDefinition) {
        GraphQLType type = TypeFromAST.getTypeFromAST(schema, variableDefinition.getType());
        addInputType(type instanceof GraphQLInputType ? (GraphQLInputType) type : null);
    }

    private void enterImpl(Argument argument) {
        GraphQLArgument argumentType = null;
        if (getDirective() != null) {
            argumentType = find(getDirective().getArguments(), argument.getName());
        } else if (getFieldDef() != null) {
            argumentType = find(getFieldDef().getArguments(), argument.getName());
        }

        addInputType(argumentType != null ? argumentType.getType() : null);
        this.argument = argumentType;
    }

    private void enterImpl(ArrayValue arrayValue) {
        GraphQLNullableType nullableType = getNullableType(getInputType());
        GraphQLInputType inputType = null;
        if (isList(nullableType)) {
            inputType = (GraphQLInputType) unwrapOne(nullableType);
        }
        addInputType(inputType);
    }

    private void enterImpl(ObjectField objectField) {
        GraphQLUnmodifiedType objectType = unwrapAll(getInputType());
        GraphQLInputType inputType = null;
        if (objectType instanceof GraphQLInputObjectType) {
            GraphQLInputObjectType inputObjectType = (GraphQLInputObjectType) objectType;
            GraphQLInputObjectField inputField = schema.getFieldVisibility().getFieldDefinition(inputObjectType, objectField.getName());
            if (inputField != null)
                inputType = inputField.getType();
        }
        addInputType(inputType);
    }

    private GraphQLArgument find(List<GraphQLArgument> arguments, String name) {
        for (GraphQLArgument argument : arguments) {
            if (argument.getName().equals(name)) return argument;
        }
        return null;
    }


    @Override
    public void leave(Node node, List<Node> ancestors) {
        if (node instanceof OperationDefinition) {
            outputTypeStack.remove(outputTypeStack.size() - 1);
        } else if (node instanceof SelectionSet) {
            parentTypeStack.remove(parentTypeStack.size() - 1);
        } else if (node instanceof Field) {
            leaveName(((Field) node).getName());
            fieldDefStack.remove(fieldDefStack.size() - 1);
            outputTypeStack.remove(outputTypeStack.size() - 1);
        } else if (node instanceof Directive) {
            directive = null;
        } else if (node instanceof InlineFragment) {
            outputTypeStack.remove(outputTypeStack.size() - 1);
        } else if (node instanceof FragmentDefinition) {
            leaveName(((FragmentDefinition) node).getName());
            outputTypeStack.remove(outputTypeStack.size() - 1);
        } else if (node instanceof VariableDefinition) {
            inputTypeStack.remove(inputTypeStack.size() - 1);
        } else if (node instanceof Argument) {
            argument = null;
            inputTypeStack.remove(inputTypeStack.size() - 1);
        } else if (node instanceof ArrayValue) {
            inputTypeStack.remove(inputTypeStack.size() - 1);
        } else if (node instanceof ObjectField) {
            inputTypeStack.remove(inputTypeStack.size() - 1);
        }
    }

    private void enterName(String name) {
        if (!isEmpty(name)) {
            nameStack.add(name);
        }
    }

    private void leaveName(String name) {
        if (!isEmpty(name)) {
            nameStack.remove(nameStack.size() - 1);
        }
    }

    private boolean isEmpty(String name) {
        return name == null || name.isEmpty();
    }

    private GraphQLNullableType getNullableType(GraphQLType type) {
        return (GraphQLNullableType) (isNonNull(type) ? unwrapOne(type) : type);
    }

    /**
     * @return can be null if current node does not have a OutputType associated: for example
     * if the current field is unknown
     */
    public GraphQLOutputType getOutputType() {
        return lastElement(outputTypeStack);
    }

    private void addOutputType(GraphQLOutputType type) {
        outputTypeStack.add(type);
    }


    private <T> T lastElement(List<T> list) {
        if (list.size() == 0) return null;
        return list.get(list.size() - 1);
    }

    /**
     * @return can be null if the parent is not a CompositeType
     */
    public GraphQLCompositeType getParentType() {
        return lastElement(parentTypeStack);
    }

    private void addParentType(GraphQLCompositeType compositeType) {
        parentTypeStack.add(compositeType);
    }

    public GraphQLInputType getInputType() {
        return lastElement(inputTypeStack);
    }

    private void addInputType(GraphQLInputType graphQLInputType) {
        inputTypeStack.add(graphQLInputType);
    }

    public GraphQLFieldDefinition getFieldDef() {
        return lastElement(fieldDefStack);
    }

    public List<String> getQueryPath() {
        if (nameStack.isEmpty()) {
            return null;
        }
        return new ArrayList<>(nameStack);
    }

    private void addFieldDef(GraphQLFieldDefinition fieldDefinition) {
        fieldDefStack.add(fieldDefinition);
    }

    public GraphQLDirective getDirective() {
        return directive;
    }

    public GraphQLArgument getArgument() {
        return argument;
    }


    private GraphQLFieldDefinition getFieldDef(GraphQLSchema schema, GraphQLType parentType, Field field) {
        if (schema.getQueryType().equals(parentType)) {
            if (field.getName().equals(SchemaMetaFieldDef.getName())) {
                return SchemaMetaFieldDef;
            }
            if (field.getName().equals(TypeMetaFieldDef.getName())) {
                return TypeMetaFieldDef;
            }
        }
        if (field.getName().equals(TypeNameMetaFieldDef.getName())
                && (parentType instanceof GraphQLObjectType ||
                parentType instanceof GraphQLInterfaceType ||
                parentType instanceof GraphQLUnionType)) {
            return TypeNameMetaFieldDef;
        }
        if (parentType instanceof GraphQLFieldsContainer) {
            return schema.getFieldVisibility().getFieldDefinition((GraphQLFieldsContainer) parentType, field.getName());
        }
        return null;
    }
}
