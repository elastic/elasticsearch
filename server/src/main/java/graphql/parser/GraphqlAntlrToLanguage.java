package graphql.parser;


import graphql.Assert;
import graphql.Internal;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.Comment;
import graphql.language.Definition;
import graphql.language.Description;
import graphql.language.Directive;
import graphql.language.DirectiveDefinition;
import graphql.language.DirectiveLocation;
import graphql.language.Document;
import graphql.language.EnumTypeDefinition;
import graphql.language.EnumTypeExtensionDefinition;
import graphql.language.EnumValue;
import graphql.language.EnumValueDefinition;
import graphql.language.Field;
import graphql.language.FieldDefinition;
import graphql.language.FloatValue;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.IgnoredChar;
import graphql.language.IgnoredChars;
import graphql.language.InlineFragment;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputObjectTypeExtensionDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.IntValue;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.InterfaceTypeExtensionDefinition;
import graphql.language.ListType;
import graphql.language.NodeBuilder;
import graphql.language.NonNullType;
import graphql.language.ObjectField;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.ObjectValue;
import graphql.language.OperationDefinition;
import graphql.language.OperationTypeDefinition;
import graphql.language.SDLDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.ScalarTypeExtensionDefinition;
import graphql.language.SchemaDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.SourceLocation;
import graphql.language.StringValue;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.language.UnionTypeDefinition;
import graphql.language.UnionTypeExtensionDefinition;
import graphql.language.Value;
import graphql.language.VariableDefinition;
import graphql.language.VariableReference;
import graphql.parser.antlr.GraphqlLexer;
import graphql.parser.antlr.GraphqlParser;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static graphql.Assert.assertShouldNeverHappen;
import static graphql.language.NullValue.Null;
import static graphql.parser.StringValueParsing.parseSingleQuotedString;
import static graphql.parser.StringValueParsing.parseTripleQuotedString;
import static java.util.stream.Collectors.toList;

@Internal
public class GraphqlAntlrToLanguage {

    private static final int CHANNEL_COMMENTS = 2;
    private static final int CHANNEL_IGNORED_CHARS = 3;
    private final CommonTokenStream tokens;
    private final MultiSourceReader multiSourceReader;


    public GraphqlAntlrToLanguage(CommonTokenStream tokens, MultiSourceReader multiSourceReader) {
        this.tokens = tokens;
        this.multiSourceReader = multiSourceReader;
    }

    //MARKER START: Here GraphqlOperation.g4 specific methods begin


    public Document createDocument(GraphqlParser.DocumentContext ctx) {
        Document.Builder document = Document.newDocument();
        addCommonData(document, ctx);
        document.definitions(ctx.definition().stream().map(this::createDefinition)
                .collect(toList()));
        return document.build();
    }

    protected Definition createDefinition(GraphqlParser.DefinitionContext definitionContext) {
        if (definitionContext.operationDefinition() != null) {
            return createOperationDefinition(definitionContext.operationDefinition());
        } else if (definitionContext.fragmentDefinition() != null) {
            return createFragmentDefinition(definitionContext.fragmentDefinition());
        } else if (definitionContext.typeSystemDefinition() != null) {
            return createTypeSystemDefinition(definitionContext.typeSystemDefinition());
        } else {
            return assertShouldNeverHappen();
        }

    }

    protected OperationDefinition createOperationDefinition(GraphqlParser.OperationDefinitionContext ctx) {
        OperationDefinition.Builder operationDefinition = OperationDefinition.newOperationDefinition();
        addCommonData(operationDefinition, ctx);
        if (ctx.operationType() == null) {
            operationDefinition.operation(OperationDefinition.Operation.QUERY);
        } else {
            operationDefinition.operation(parseOperation(ctx.operationType()));
        }
        if (ctx.name() != null) {
            operationDefinition.name(ctx.name().getText());
        }
        operationDefinition.variableDefinitions(createVariableDefinitions(ctx.variableDefinitions()));
        operationDefinition.selectionSet(createSelectionSet(ctx.selectionSet()));
        operationDefinition.directives(createDirectives(ctx.directives()));
        return operationDefinition.build();
    }

    protected OperationDefinition.Operation parseOperation(GraphqlParser.OperationTypeContext operationTypeContext) {
        switch (operationTypeContext.getText()) {
            case "query":
                return OperationDefinition.Operation.QUERY;
            case "mutation":
                return OperationDefinition.Operation.MUTATION;
            case "subscription":
                return OperationDefinition.Operation.SUBSCRIPTION;
            default:
                return assertShouldNeverHappen("InternalError: unknown operationTypeContext=%s", operationTypeContext.getText());
        }
    }

    protected FragmentSpread createFragmentSpread(GraphqlParser.FragmentSpreadContext ctx) {
        FragmentSpread.Builder fragmentSpread = FragmentSpread.newFragmentSpread().name(ctx.fragmentName().getText());
        addCommonData(fragmentSpread, ctx);
        fragmentSpread.directives(createDirectives(ctx.directives()));
        return fragmentSpread.build();
    }

    protected List<VariableDefinition> createVariableDefinitions(GraphqlParser.VariableDefinitionsContext ctx) {
        if (ctx == null) {
            return new ArrayList<>();
        }
        return ctx.variableDefinition().stream().map(this::createVariableDefinition).collect(toList());
    }

    protected VariableDefinition createVariableDefinition(GraphqlParser.VariableDefinitionContext ctx) {
        VariableDefinition.Builder variableDefinition = VariableDefinition.newVariableDefinition();
        addCommonData(variableDefinition, ctx);
        variableDefinition.name(ctx.variable().name().getText());
        if (ctx.defaultValue() != null) {
            Value value = createValue(ctx.defaultValue().value());
            variableDefinition.defaultValue(value);
        }
        variableDefinition.type(createType(ctx.type()));
        return variableDefinition.build();

    }

    protected FragmentDefinition createFragmentDefinition(GraphqlParser.FragmentDefinitionContext ctx) {
        FragmentDefinition.Builder fragmentDefinition = FragmentDefinition.newFragmentDefinition();
        addCommonData(fragmentDefinition, ctx);
        fragmentDefinition.name(ctx.fragmentName().getText());
        fragmentDefinition.typeCondition(TypeName.newTypeName().name(ctx.typeCondition().typeName().getText()).build());
        fragmentDefinition.directives(createDirectives(ctx.directives()));
        fragmentDefinition.selectionSet(createSelectionSet(ctx.selectionSet()));
        return fragmentDefinition.build();
    }


    protected SelectionSet createSelectionSet(GraphqlParser.SelectionSetContext ctx) {
        if (ctx == null) {
            return null;
        }
        SelectionSet.Builder builder = SelectionSet.newSelectionSet();
        addCommonData(builder, ctx);
        List<Selection> selections = ctx.selection().stream().map(selectionContext -> {
            if (selectionContext.field() != null) {
                return createField(selectionContext.field());
            }
            if (selectionContext.fragmentSpread() != null) {
                return createFragmentSpread(selectionContext.fragmentSpread());
            }
            if (selectionContext.inlineFragment() != null) {
                return createInlineFragment(selectionContext.inlineFragment());
            }
            return (Selection) Assert.assertShouldNeverHappen();

        }).collect(toList());
        builder.selections(selections);
        return builder.build();
    }


    protected Field createField(GraphqlParser.FieldContext ctx) {
        Field.Builder builder = Field.newField();
        addCommonData(builder, ctx);
        builder.name(ctx.name().getText());
        if (ctx.alias() != null) {
            builder.alias(ctx.alias().name().getText());
        }

        builder.directives(createDirectives(ctx.directives()));
        builder.arguments(createArguments(ctx.arguments()));
        builder.selectionSet(createSelectionSet(ctx.selectionSet()));
        return builder.build();
    }


    protected InlineFragment createInlineFragment(GraphqlParser.InlineFragmentContext ctx) {
        InlineFragment.Builder inlineFragment = InlineFragment.newInlineFragment();
        addCommonData(inlineFragment, ctx);
        if (ctx.typeCondition() != null) {
            inlineFragment.typeCondition(createTypeName(ctx.typeCondition().typeName()));
        }
        inlineFragment.directives(createDirectives(ctx.directives()));
        inlineFragment.selectionSet(createSelectionSet(ctx.selectionSet()));
        return inlineFragment.build();
    }

    //MARKER END: Here GraphqlOperation.g4 specific methods end

    protected SDLDefinition createTypeSystemDefinition(GraphqlParser.TypeSystemDefinitionContext ctx) {
        if (ctx.schemaDefinition() != null) {
            return createSchemaDefinition(ctx.schemaDefinition());
        } else if (ctx.directiveDefinition() != null) {
            return createDirectiveDefinition(ctx.directiveDefinition());
        } else if (ctx.typeDefinition() != null) {
            return createTypeDefinition(ctx.typeDefinition());
        } else if (ctx.typeExtension() != null) {
            return createTypeExtension(ctx.typeExtension());
        } else {
            return assertShouldNeverHappen();
        }
    }

    protected TypeDefinition createTypeExtension(GraphqlParser.TypeExtensionContext ctx) {
        if (ctx.enumTypeExtensionDefinition() != null) {
            return createEnumTypeExtensionDefinition(ctx.enumTypeExtensionDefinition());

        } else if (ctx.objectTypeExtensionDefinition() != null) {
            return createObjectTypeExtensionDefinition(ctx.objectTypeExtensionDefinition());

        } else if (ctx.inputObjectTypeExtensionDefinition() != null) {
            return createInputObjectTypeExtensionDefinition(ctx.inputObjectTypeExtensionDefinition());

        } else if (ctx.interfaceTypeExtensionDefinition() != null) {
            return createInterfaceTypeExtensionDefinition(ctx.interfaceTypeExtensionDefinition());

        } else if (ctx.scalarTypeExtensionDefinition() != null) {
            return createScalarTypeExtensionDefinition(ctx.scalarTypeExtensionDefinition());

        } else if (ctx.unionTypeExtensionDefinition() != null) {
            return createUnionTypeExtensionDefinition(ctx.unionTypeExtensionDefinition());
        } else {
            return assertShouldNeverHappen();
        }
    }

    protected TypeDefinition createTypeDefinition(GraphqlParser.TypeDefinitionContext ctx) {
        if (ctx.enumTypeDefinition() != null) {
            return createEnumTypeDefinition(ctx.enumTypeDefinition());

        } else if (ctx.objectTypeDefinition() != null) {
            return createObjectTypeDefinition(ctx.objectTypeDefinition());

        } else if (ctx.inputObjectTypeDefinition() != null) {
            return createInputObjectTypeDefinition(ctx.inputObjectTypeDefinition());

        } else if (ctx.interfaceTypeDefinition() != null) {
            return createInterfaceTypeDefinition(ctx.interfaceTypeDefinition());

        } else if (ctx.scalarTypeDefinition() != null) {
            return createScalarTypeDefinition(ctx.scalarTypeDefinition());

        } else if (ctx.unionTypeDefinition() != null) {
            return createUnionTypeDefinition(ctx.unionTypeDefinition());

        } else {
            return assertShouldNeverHappen();
        }
    }


    protected Type createType(GraphqlParser.TypeContext ctx) {
        if (ctx.typeName() != null) {
            return createTypeName(ctx.typeName());
        } else if (ctx.nonNullType() != null) {
            return createNonNullType(ctx.nonNullType());
        } else if (ctx.listType() != null) {
            return createListType(ctx.listType());
        } else {
            return assertShouldNeverHappen();
        }
    }

    protected TypeName createTypeName(GraphqlParser.TypeNameContext ctx) {
        TypeName.Builder builder = TypeName.newTypeName();
        builder.name(ctx.name().getText());
        addCommonData(builder, ctx);
        return builder.build();
    }

    protected NonNullType createNonNullType(GraphqlParser.NonNullTypeContext ctx) {
        NonNullType.Builder builder = NonNullType.newNonNullType();
        addCommonData(builder, ctx);
        if (ctx.listType() != null) {
            builder.type(createListType(ctx.listType()));
        } else if (ctx.typeName() != null) {
            builder.type(createTypeName(ctx.typeName()));
        } else {
            return assertShouldNeverHappen();
        }
        return builder.build();
    }

    protected ListType createListType(GraphqlParser.ListTypeContext ctx) {
        ListType.Builder builder = ListType.newListType();
        addCommonData(builder, ctx);
        builder.type(createType(ctx.type()));
        return builder.build();
    }

    protected Argument createArgument(GraphqlParser.ArgumentContext ctx) {
        Argument.Builder builder = Argument.newArgument();
        addCommonData(builder, ctx);
        builder.name(ctx.name().getText());
        builder.value(createValue(ctx.valueWithVariable()));
        return builder.build();
    }

    protected List<Argument> createArguments(GraphqlParser.ArgumentsContext ctx) {
        if (ctx == null) {
            return new ArrayList<>();
        }
        return ctx.argument().stream().map(this::createArgument).collect(toList());
    }


    protected List<Directive> createDirectives(GraphqlParser.DirectivesContext ctx) {
        if (ctx == null) {
            return new ArrayList<>();
        }
        return ctx.directive().stream().map(this::createDirective).collect(toList());
    }

    protected Directive createDirective(GraphqlParser.DirectiveContext ctx) {
        Directive.Builder builder = Directive.newDirective();
        builder.name(ctx.name().getText());
        addCommonData(builder, ctx);
        builder.arguments(createArguments(ctx.arguments()));
        return builder.build();
    }

    protected SchemaDefinition createSchemaDefinition(GraphqlParser.SchemaDefinitionContext ctx) {
        SchemaDefinition.Builder def = SchemaDefinition.newSchemaDefinition();
        addCommonData(def, ctx);
        def.directives(createDirectives(ctx.directives()));
        def.operationTypeDefinitions(ctx.operationTypeDefinition().stream()
                .map(this::createOperationTypeDefinition).collect(toList()));
        return def.build();
    }

    protected OperationTypeDefinition createOperationTypeDefinition(GraphqlParser.OperationTypeDefinitionContext ctx) {
        OperationTypeDefinition.Builder def = OperationTypeDefinition.newOperationTypeDefinition();
        def.name(ctx.operationType().getText());
        def.typeName(createTypeName(ctx.typeName()));
        addCommonData(def, ctx);
        return def.build();
    }

    protected ScalarTypeDefinition createScalarTypeDefinition(GraphqlParser.ScalarTypeDefinitionContext ctx) {
        ScalarTypeDefinition.Builder def = ScalarTypeDefinition.newScalarTypeDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        return def.build();
    }

    protected ScalarTypeExtensionDefinition createScalarTypeExtensionDefinition(GraphqlParser.ScalarTypeExtensionDefinitionContext ctx) {
        ScalarTypeExtensionDefinition.Builder def = ScalarTypeExtensionDefinition.newScalarTypeExtensionDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.directives(createDirectives(ctx.directives()));
        return def.build();
    }

    protected ObjectTypeDefinition createObjectTypeDefinition(GraphqlParser.ObjectTypeDefinitionContext ctx) {
        ObjectTypeDefinition.Builder def = ObjectTypeDefinition.newObjectTypeDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        GraphqlParser.ImplementsInterfacesContext implementsInterfacesContext = ctx.implementsInterfaces();
        List<Type> implementz = new ArrayList<>();
        while (implementsInterfacesContext != null) {
            List<TypeName> typeNames = implementsInterfacesContext.typeName().stream().map(this::createTypeName).collect(toList());
            implementz.addAll(0, typeNames);
            implementsInterfacesContext = implementsInterfacesContext.implementsInterfaces();
        }
        def.implementz(implementz);
        if (ctx.fieldsDefinition() != null) {
            def.fieldDefinitions(createFieldDefinitions(ctx.fieldsDefinition()));
        }
        return def.build();
    }

    protected ObjectTypeExtensionDefinition createObjectTypeExtensionDefinition(GraphqlParser.ObjectTypeExtensionDefinitionContext ctx) {
        ObjectTypeExtensionDefinition.Builder def = ObjectTypeExtensionDefinition.newObjectTypeExtensionDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.directives(createDirectives(ctx.directives()));
        GraphqlParser.ImplementsInterfacesContext implementsInterfacesContext = ctx.implementsInterfaces();
        List<Type> implementz = new ArrayList<>();
        while (implementsInterfacesContext != null) {
            List<TypeName> typeNames = implementsInterfacesContext.typeName().stream().map(this::createTypeName).collect(toList());
            implementz.addAll(0, typeNames);
            implementsInterfacesContext = implementsInterfacesContext.implementsInterfaces();
        }
        def.implementz(implementz);
        if (ctx.extensionFieldsDefinition() != null) {
            def.fieldDefinitions(createFieldDefinitions(ctx.extensionFieldsDefinition()));
        }
        return def.build();
    }

    protected List<FieldDefinition> createFieldDefinitions(GraphqlParser.FieldsDefinitionContext ctx) {
        if (ctx == null) {
            return new ArrayList<>();
        }
        return ctx.fieldDefinition().stream().map(this::createFieldDefinition).collect(toList());
    }

    protected List<FieldDefinition> createFieldDefinitions(GraphqlParser.ExtensionFieldsDefinitionContext ctx) {
        if (ctx == null) {
            return new ArrayList<>();
        }
        return ctx.fieldDefinition().stream().map(this::createFieldDefinition).collect(toList());
    }


    protected FieldDefinition createFieldDefinition(GraphqlParser.FieldDefinitionContext ctx) {
        FieldDefinition.Builder def = FieldDefinition.newFieldDefinition();
        def.name(ctx.name().getText());
        def.type(createType(ctx.type()));
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        if (ctx.argumentsDefinition() != null) {
            def.inputValueDefinitions(createInputValueDefinitions(ctx.argumentsDefinition().inputValueDefinition()));
        }
        return def.build();
    }

    protected List<InputValueDefinition> createInputValueDefinitions(List<GraphqlParser.InputValueDefinitionContext> defs) {
        return defs.stream().map(this::createInputValueDefinition).collect(toList());

    }

    protected InputValueDefinition createInputValueDefinition(GraphqlParser.InputValueDefinitionContext ctx) {
        InputValueDefinition.Builder def = InputValueDefinition.newInputValueDefinition();
        def.name(ctx.name().getText());
        def.type(createType(ctx.type()));
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        if (ctx.defaultValue() != null) {
            def.defaultValue(createValue(ctx.defaultValue().value()));
        }
        def.directives(createDirectives(ctx.directives()));
        return def.build();
    }

    protected InterfaceTypeDefinition createInterfaceTypeDefinition(GraphqlParser.InterfaceTypeDefinitionContext ctx) {
        InterfaceTypeDefinition.Builder def = InterfaceTypeDefinition.newInterfaceTypeDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        def.definitions(createFieldDefinitions(ctx.fieldsDefinition()));
        return def.build();
    }

    protected InterfaceTypeExtensionDefinition createInterfaceTypeExtensionDefinition(GraphqlParser.InterfaceTypeExtensionDefinitionContext ctx) {
        InterfaceTypeExtensionDefinition.Builder def = InterfaceTypeExtensionDefinition.newInterfaceTypeExtensionDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.directives(createDirectives(ctx.directives()));
        def.definitions(createFieldDefinitions(ctx.extensionFieldsDefinition()));
        return def.build();
    }

    protected UnionTypeDefinition createUnionTypeDefinition(GraphqlParser.UnionTypeDefinitionContext ctx) {
        UnionTypeDefinition.Builder def = UnionTypeDefinition.newUnionTypeDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        List<Type> members = new ArrayList<>();
        GraphqlParser.UnionMembershipContext unionMembership = ctx.unionMembership();
        if (unionMembership != null) {
            GraphqlParser.UnionMembersContext unionMembersContext = unionMembership.unionMembers();
            while (unionMembersContext != null) {
                members.add(0, createTypeName(unionMembersContext.typeName()));
                unionMembersContext = unionMembersContext.unionMembers();
            }
        }
        def.memberTypes(members);
        return def.build();
    }

    protected UnionTypeExtensionDefinition createUnionTypeExtensionDefinition(GraphqlParser.UnionTypeExtensionDefinitionContext ctx) {
        UnionTypeExtensionDefinition.Builder def = UnionTypeExtensionDefinition.newUnionTypeExtensionDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.directives(createDirectives(ctx.directives()));
        List<Type> members = new ArrayList<>();
        if (ctx.unionMembership() != null) {
            GraphqlParser.UnionMembersContext unionMembersContext = ctx.unionMembership().unionMembers();
            while (unionMembersContext != null) {
                members.add(0, createTypeName(unionMembersContext.typeName()));
                unionMembersContext = unionMembersContext.unionMembers();
            }
            def.memberTypes(members);
        }
        return def.build();
    }

    protected EnumTypeDefinition createEnumTypeDefinition(GraphqlParser.EnumTypeDefinitionContext ctx) {
        EnumTypeDefinition.Builder def = EnumTypeDefinition.newEnumTypeDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        if (ctx.enumValueDefinitions() != null) {
            def.enumValueDefinitions(
                    ctx.enumValueDefinitions().enumValueDefinition().stream().map(this::createEnumValueDefinition).collect(toList()));
        }
        return def.build();
    }

    protected EnumTypeExtensionDefinition createEnumTypeExtensionDefinition(GraphqlParser.EnumTypeExtensionDefinitionContext ctx) {
        EnumTypeExtensionDefinition.Builder def = EnumTypeExtensionDefinition.newEnumTypeExtensionDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.directives(createDirectives(ctx.directives()));
        if (ctx.extensionEnumValueDefinitions() != null) {
            def.enumValueDefinitions(
                    ctx.extensionEnumValueDefinitions().enumValueDefinition().stream().map(this::createEnumValueDefinition).collect(toList()));
        }
        return def.build();
    }

    protected EnumValueDefinition createEnumValueDefinition(GraphqlParser.EnumValueDefinitionContext ctx) {
        EnumValueDefinition.Builder def = EnumValueDefinition.newEnumValueDefinition();
        def.name(ctx.enumValue().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        return def.build();
    }

    protected InputObjectTypeDefinition createInputObjectTypeDefinition(GraphqlParser.InputObjectTypeDefinitionContext ctx) {
        InputObjectTypeDefinition.Builder def = InputObjectTypeDefinition.newInputObjectDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        def.directives(createDirectives(ctx.directives()));
        if (ctx.inputObjectValueDefinitions() != null) {
            def.inputValueDefinitions(createInputValueDefinitions(ctx.inputObjectValueDefinitions().inputValueDefinition()));
        }
        return def.build();
    }

    protected InputObjectTypeExtensionDefinition createInputObjectTypeExtensionDefinition(GraphqlParser.InputObjectTypeExtensionDefinitionContext ctx) {
        InputObjectTypeExtensionDefinition.Builder def = InputObjectTypeExtensionDefinition.newInputObjectTypeExtensionDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.directives(createDirectives(ctx.directives()));
        if (ctx.extensionInputObjectValueDefinitions() != null) {
            def.inputValueDefinitions(createInputValueDefinitions(ctx.extensionInputObjectValueDefinitions().inputValueDefinition()));
        }
        return def.build();
    }

    protected DirectiveDefinition createDirectiveDefinition(GraphqlParser.DirectiveDefinitionContext ctx) {
        DirectiveDefinition.Builder def = DirectiveDefinition.newDirectiveDefinition();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        def.description(newDescription(ctx.description()));
        GraphqlParser.DirectiveLocationsContext directiveLocationsContext = ctx.directiveLocations();
        List<DirectiveLocation> directiveLocations = new ArrayList<>();
        while (directiveLocationsContext != null) {
            directiveLocations.add(0, createDirectiveLocation(directiveLocationsContext.directiveLocation()));
            directiveLocationsContext = directiveLocationsContext.directiveLocations();
        }
        def.directiveLocations(directiveLocations);
        if (ctx.argumentsDefinition() != null) {
            def.inputValueDefinitions(createInputValueDefinitions(ctx.argumentsDefinition().inputValueDefinition()));
        }
        return def.build();
    }

    protected DirectiveLocation createDirectiveLocation(GraphqlParser.DirectiveLocationContext ctx) {
        DirectiveLocation.Builder def = DirectiveLocation.newDirectiveLocation();
        def.name(ctx.name().getText());
        addCommonData(def, ctx);
        return def.build();
    }

    protected Value createValue(GraphqlParser.ValueWithVariableContext ctx) {
        if (ctx.IntValue() != null) {
            IntValue.Builder intValue = IntValue.newIntValue().value(new BigInteger(ctx.IntValue().getText()));
            addCommonData(intValue, ctx);
            return intValue.build();
        } else if (ctx.FloatValue() != null) {
            FloatValue.Builder floatValue = FloatValue.newFloatValue().value(new BigDecimal(ctx.FloatValue().getText()));
            addCommonData(floatValue, ctx);
            return floatValue.build();
        } else if (ctx.BooleanValue() != null) {
            BooleanValue.Builder booleanValue = BooleanValue.newBooleanValue().value(Boolean.parseBoolean(ctx.BooleanValue().getText()));
            addCommonData(booleanValue, ctx);
            return booleanValue.build();
        } else if (ctx.NullValue() != null) {
            return Null;
        } else if (ctx.stringValue() != null) {
            StringValue.Builder stringValue = StringValue.newStringValue().value(quotedString(ctx.stringValue()));
            addCommonData(stringValue, ctx);
            return stringValue.build();
        } else if (ctx.enumValue() != null) {
            EnumValue.Builder enumValue = EnumValue.newEnumValue().name(ctx.enumValue().getText());
            addCommonData(enumValue, ctx);
            return enumValue.build();
        } else if (ctx.arrayValueWithVariable() != null) {
            ArrayValue.Builder arrayValue = ArrayValue.newArrayValue();
            addCommonData(arrayValue, ctx);
            List<Value> values = new ArrayList<>();
            for (GraphqlParser.ValueWithVariableContext valueWithVariableContext : ctx.arrayValueWithVariable().valueWithVariable()) {
                values.add(createValue(valueWithVariableContext));
            }
            return arrayValue.values(values).build();
        } else if (ctx.objectValueWithVariable() != null) {
            ObjectValue.Builder objectValue = ObjectValue.newObjectValue();
            addCommonData(objectValue, ctx);
            List<ObjectField> objectFields = new ArrayList<>();
            for (GraphqlParser.ObjectFieldWithVariableContext objectFieldWithVariableContext :
                    ctx.objectValueWithVariable().objectFieldWithVariable()) {

                ObjectField objectField = ObjectField.newObjectField()
                        .name(objectFieldWithVariableContext.name().getText())
                        .value(createValue(objectFieldWithVariableContext.valueWithVariable()))
                        .build();
                objectFields.add(objectField);
            }
            return objectValue.objectFields(objectFields).build();
        } else if (ctx.variable() != null) {
            VariableReference.Builder variableReference = VariableReference.newVariableReference().name(ctx.variable().name().getText());
            addCommonData(variableReference, ctx);
            return variableReference.build();
        }
        return assertShouldNeverHappen();
    }

    protected Value createValue(GraphqlParser.ValueContext ctx) {
        if (ctx.IntValue() != null) {
            IntValue.Builder intValue = IntValue.newIntValue().value(new BigInteger(ctx.IntValue().getText()));
            addCommonData(intValue, ctx);
            return intValue.build();
        } else if (ctx.FloatValue() != null) {
            FloatValue.Builder floatValue = FloatValue.newFloatValue().value(new BigDecimal(ctx.FloatValue().getText()));
            addCommonData(floatValue, ctx);
            return floatValue.build();
        } else if (ctx.BooleanValue() != null) {
            BooleanValue.Builder booleanValue = BooleanValue.newBooleanValue().value(Boolean.parseBoolean(ctx.BooleanValue().getText()));
            addCommonData(booleanValue, ctx);
            return booleanValue.build();
        } else if (ctx.NullValue() != null) {
            return Null;
        } else if (ctx.stringValue() != null) {
            StringValue.Builder stringValue = StringValue.newStringValue().value(quotedString(ctx.stringValue()));
            addCommonData(stringValue, ctx);
            return stringValue.build();
        } else if (ctx.enumValue() != null) {
            EnumValue.Builder enumValue = EnumValue.newEnumValue().name(ctx.enumValue().getText());
            addCommonData(enumValue, ctx);
            return enumValue.build();
        } else if (ctx.arrayValue() != null) {
            ArrayValue.Builder arrayValue = ArrayValue.newArrayValue();
            addCommonData(arrayValue, ctx);
            List<Value> values = new ArrayList<>();
            for (GraphqlParser.ValueContext valueContext : ctx.arrayValue().value()) {
                values.add(createValue(valueContext));
            }
            return arrayValue.values(values).build();
        } else if (ctx.objectValue() != null) {
            ObjectValue.Builder objectValue = ObjectValue.newObjectValue();
            addCommonData(objectValue, ctx);
            List<ObjectField> objectFields = new ArrayList<>();
            for (GraphqlParser.ObjectFieldContext objectFieldContext :
                    ctx.objectValue().objectField()) {
                ObjectField objectField = ObjectField.newObjectField()
                        .name(objectFieldContext.name().getText())
                        .value(createValue(objectFieldContext.value()))
                        .build();
                objectFields.add(objectField);
            }
            return objectValue.objectFields(objectFields).build();
        }
        return assertShouldNeverHappen();
    }

    static String quotedString(GraphqlParser.StringValueContext ctx) {
        boolean multiLine = ctx.TripleQuotedStringValue() != null;
        String strText = ctx.getText();
        if (multiLine) {
            return parseTripleQuotedString(strText);
        } else {
            return parseSingleQuotedString(strText);
        }
    }

    protected void addCommonData(NodeBuilder nodeBuilder, ParserRuleContext parserRuleContext) {
        List<Comment> comments = getComments(parserRuleContext);
        if (!comments.isEmpty()) {
            nodeBuilder.comments(comments);
        }
        nodeBuilder.sourceLocation(getSourceLocation(parserRuleContext));
        addIgnoredChars(parserRuleContext, nodeBuilder);
    }

    private void addIgnoredChars(ParserRuleContext ctx, NodeBuilder nodeBuilder) {
        Token start = ctx.getStart();
        int tokenStartIndex = start.getTokenIndex();
        List<Token> leftChannel = tokens.getHiddenTokensToLeft(tokenStartIndex, CHANNEL_IGNORED_CHARS);
        List<IgnoredChar> ignoredCharsLeft = mapTokenToIgnoredChar(leftChannel);

        Token stop = ctx.getStop();
        int tokenStopIndex = stop.getTokenIndex();
        List<Token> rightChannel = tokens.getHiddenTokensToRight(tokenStopIndex, CHANNEL_IGNORED_CHARS);
        List<IgnoredChar> ignoredCharsRight = mapTokenToIgnoredChar(rightChannel);

        nodeBuilder.ignoredChars(new IgnoredChars(ignoredCharsLeft, ignoredCharsRight));
    }

    private List<IgnoredChar> mapTokenToIgnoredChar(List<Token> tokens) {
        if (tokens == null) {
            return Collections.emptyList();
        }
        return tokens
                .stream()
                .map(this::createIgnoredChar)
                .collect(toList());

    }

    private IgnoredChar createIgnoredChar(Token token) {
        String symbolicName = GraphqlLexer.VOCABULARY.getSymbolicName(token.getType());
        IgnoredChar.IgnoredCharKind kind;
        switch (symbolicName) {
            case "CR":
                kind = IgnoredChar.IgnoredCharKind.CR;
                break;
            case "LF":
                kind = IgnoredChar.IgnoredCharKind.LF;
                break;
            case "Tab":
                kind = IgnoredChar.IgnoredCharKind.TAB;
                break;
            case "Comma":
                kind = IgnoredChar.IgnoredCharKind.COMMA;
                break;
            case "Space":
                kind = IgnoredChar.IgnoredCharKind.SPACE;
                break;
            default:
                kind = IgnoredChar.IgnoredCharKind.OTHER;
        }
        return new IgnoredChar(token.getText(), kind, getSourceLocation(token));
    }

    protected Description newDescription(GraphqlParser.DescriptionContext descriptionCtx) {
        if (descriptionCtx == null) {
            return null;
        }
        GraphqlParser.StringValueContext stringValueCtx = descriptionCtx.stringValue();
        if (stringValueCtx == null) {
            return null;
        }
        boolean multiLine = stringValueCtx.TripleQuotedStringValue() != null;
        String content = stringValueCtx.getText();
        if (multiLine) {
            content = parseTripleQuotedString(content);
        } else {
            content = parseSingleQuotedString(content);
        }
        SourceLocation sourceLocation = getSourceLocation(descriptionCtx);
        return new Description(content, sourceLocation, multiLine);
    }

    protected SourceLocation getSourceLocation(Token token) {
        return SourceLocationHelper.mkSourceLocation(multiSourceReader, token);
    }

    protected SourceLocation getSourceLocation(ParserRuleContext parserRuleContext) {
        return getSourceLocation(parserRuleContext.getStart());
    }

    protected List<Comment> getComments(ParserRuleContext ctx) {
        Token start = ctx.getStart();
        if (start != null) {
            int tokPos = start.getTokenIndex();
            List<Token> refChannel = tokens.getHiddenTokensToLeft(tokPos, CHANNEL_COMMENTS);
            if (refChannel != null) {
                return getCommentOnChannel(refChannel);
            }
        }
        return Collections.emptyList();
    }


    protected List<Comment> getCommentOnChannel(List<Token> refChannel) {
        List<Comment> comments = new ArrayList<>();
        for (Token refTok : refChannel) {
            String text = refTok.getText();
            // we strip the leading hash # character but we don't trim because we don't
            // know the "comment markup".  Maybe its space sensitive, maybe its not.  So
            // consumers can decide that
            if (text == null) {
                continue;
            }
            text = text.replaceFirst("^#", "");
            MultiSourceReader.SourceAndLine sourceAndLine = multiSourceReader.getSourceAndLineFromOverallLine(refTok.getLine());
            int column = refTok.getCharPositionInLine();
            // graphql spec says line numbers start at 1
            int line = sourceAndLine.getLine() + 1;
            comments.add(new Comment(text, new SourceLocation(line, column, sourceAndLine.getSourceName())));
        }
        return comments;
    }

}
