package graphql.schema.idl;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.introspection.Introspection.DirectiveLocation;
import graphql.language.Argument;
import graphql.language.Directive;
import graphql.language.DirectiveDefinition;
import graphql.language.EnumTypeDefinition;
import graphql.language.EnumValueDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.Node;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.language.UnionTypeDefinition;
import graphql.schema.idl.errors.DirectiveIllegalLocationError;
import graphql.schema.idl.errors.DirectiveMissingNonNullArgumentError;
import graphql.schema.idl.errors.DirectiveUndeclaredError;
import graphql.schema.idl.errors.DirectiveUnknownArgumentError;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static graphql.introspection.Introspection.DirectiveLocation.ARGUMENT_DEFINITION;
import static graphql.introspection.Introspection.DirectiveLocation.ENUM;
import static graphql.introspection.Introspection.DirectiveLocation.ENUM_VALUE;
import static graphql.introspection.Introspection.DirectiveLocation.FIELD_DEFINITION;
import static graphql.introspection.Introspection.DirectiveLocation.INPUT_FIELD_DEFINITION;
import static graphql.introspection.Introspection.DirectiveLocation.INPUT_OBJECT;
import static graphql.introspection.Introspection.DirectiveLocation.INTERFACE;
import static graphql.introspection.Introspection.DirectiveLocation.OBJECT;
import static graphql.introspection.Introspection.DirectiveLocation.SCALAR;
import static graphql.introspection.Introspection.DirectiveLocation.UNION;
import static graphql.util.FpKit.getByName;
import static graphql.util.FpKit.mergeFirst;

/**
 * This is responsible for traversing EVERY type and field in the registry and ensuring that
 * any directives used follow the directive definition rules, for example
 * field directives can be used on object types
 */
@Internal
class SchemaTypeDirectivesChecker {

    private final TypeDefinitionRegistry typeRegistry;
    private final RuntimeWiring runtimeWiring;

    public SchemaTypeDirectivesChecker(final TypeDefinitionRegistry typeRegistry,
                                       final RuntimeWiring runtimeWiring) {
        this.typeRegistry = typeRegistry;
        this.runtimeWiring = runtimeWiring;
    }

    void checkTypeDirectives(List<GraphQLError> errors) {
        typeRegistry.objectTypeExtensions().values()
                .forEach(extDefinitions -> extDefinitions.forEach(ext -> checkDirectives(OBJECT, errors, ext)));
        typeRegistry.interfaceTypeExtensions().values()
                .forEach(extDefinitions -> extDefinitions.forEach(ext -> checkDirectives(INTERFACE, errors, ext)));
        typeRegistry.unionTypeExtensions().values()
                .forEach(extDefinitions -> extDefinitions.forEach(ext -> checkDirectives(UNION, errors, ext)));
        typeRegistry.enumTypeExtensions().values()
                .forEach(extDefinitions -> extDefinitions.forEach(ext -> checkDirectives(ENUM, errors, ext)));
        typeRegistry.scalarTypeExtensions().values()
                .forEach(extDefinitions -> extDefinitions.forEach(ext -> checkDirectives(SCALAR, errors, ext)));
        typeRegistry.inputObjectTypeExtensions().values()
                .forEach(extDefinitions -> extDefinitions.forEach(ext -> checkDirectives(INPUT_OBJECT, errors, ext)));

        typeRegistry.getTypes(ObjectTypeDefinition.class)
                .forEach(typeDef -> checkDirectives(OBJECT, errors, typeDef));
        typeRegistry.getTypes(InterfaceTypeDefinition.class)
                .forEach(typeDef -> checkDirectives(INTERFACE, errors, typeDef));
        typeRegistry.getTypes(UnionTypeDefinition.class)
                .forEach(typeDef -> checkDirectives(UNION, errors, typeDef));
        typeRegistry.getTypes(EnumTypeDefinition.class)
                .forEach(typeDef -> checkDirectives(ENUM, errors, typeDef));
        typeRegistry.getTypes(InputObjectTypeDefinition.class)
                .forEach(typeDef -> checkDirectives(INPUT_OBJECT, errors, typeDef));

        typeRegistry.scalars().values()
                .forEach(typeDef -> checkDirectives(SCALAR, errors, typeDef));

    }


    private void checkDirectives(DirectiveLocation expectedLocation, List<GraphQLError> errors, TypeDefinition<?> typeDef) {
        checkDirectives(expectedLocation, errors, typeRegistry, typeDef, typeDef.getName(), typeDef.getDirectives());

        if (typeDef instanceof ObjectTypeDefinition) {
            List<FieldDefinition> fieldDefinitions = ((ObjectTypeDefinition) typeDef).getFieldDefinitions();
            checkFieldsDirectives(errors, typeRegistry, fieldDefinitions);
        }
        if (typeDef instanceof InterfaceTypeDefinition) {
            List<FieldDefinition> fieldDefinitions = ((InterfaceTypeDefinition) typeDef).getFieldDefinitions();
            checkFieldsDirectives(errors, typeRegistry, fieldDefinitions);
        }
        if (typeDef instanceof EnumTypeDefinition) {
            List<EnumValueDefinition> enumValueDefinitions = ((EnumTypeDefinition) typeDef).getEnumValueDefinitions();
            enumValueDefinitions.forEach(definition -> checkDirectives(ENUM_VALUE, errors, typeRegistry, definition, definition.getName(), definition.getDirectives()));
        }
        if (typeDef instanceof InputObjectTypeDefinition) {
            List<InputValueDefinition> inputValueDefinitions = ((InputObjectTypeDefinition) typeDef).getInputValueDefinitions();
            inputValueDefinitions.forEach(definition -> checkDirectives(INPUT_FIELD_DEFINITION, errors, typeRegistry, definition, definition.getName(), definition.getDirectives()));
        }
    }

    private void checkFieldsDirectives(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry, List<FieldDefinition> fieldDefinitions) {
        fieldDefinitions.forEach(definition -> {
            checkDirectives(FIELD_DEFINITION, errors, typeRegistry, definition, definition.getName(), definition.getDirectives());
            //
            // and check its arguments
            definition.getInputValueDefinitions().forEach(arg -> checkDirectives(ARGUMENT_DEFINITION, errors, typeRegistry, arg, arg.getName(), arg.getDirectives()));
        });
    }

    private void checkDirectives(DirectiveLocation expectedLocation, List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry, Node<?> element, String elementName, List<Directive> directives) {
        directives.forEach(directive -> {
            Optional<DirectiveDefinition> directiveDefinition = typeRegistry.getDirectiveDefinition(directive.getName());
            if (!directiveDefinition.isPresent()) {
                errors.add(new DirectiveUndeclaredError(element, elementName, directive.getName()));
            } else {
                if (!inRightLocation(expectedLocation, directiveDefinition.get())) {
                    errors.add(new DirectiveIllegalLocationError(element, elementName, directive.getName(), expectedLocation.name()));
                }
                checkDirectiveArguments(errors, typeRegistry, element, elementName, directive, directiveDefinition.get());
            }
        });
    }

    private boolean inRightLocation(DirectiveLocation expectedLocation, DirectiveDefinition directiveDefinition) {
        List<String> names = directiveDefinition.getDirectiveLocations()
                .stream().map(graphql.language.DirectiveLocation::getName)
                .map(String::toUpperCase)
                .collect(Collectors.toList());

        return names.contains(expectedLocation.name().toUpperCase());
    }

    private void checkDirectiveArguments(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry, Node element, String elementName, Directive directive, DirectiveDefinition directiveDefinition) {
        Map<String, InputValueDefinition> allowedArgs = getByName(directiveDefinition.getInputValueDefinitions(), (InputValueDefinition::getName), mergeFirst());
        Map<String, Argument> providedArgs = getByName(directive.getArguments(), (Argument::getName), mergeFirst());
        directive.getArguments().forEach(argument -> {
            InputValueDefinition allowedArg = allowedArgs.get(argument.getName());
            if (allowedArg == null) {
                errors.add(new DirectiveUnknownArgumentError(element, elementName, directive.getName(), argument.getName()));
            } else {
                ArgValueOfAllowedTypeChecker argValueOfAllowedTypeChecker = new ArgValueOfAllowedTypeChecker(directive, element, elementName, argument, typeRegistry, runtimeWiring);
                argValueOfAllowedTypeChecker.checkArgValueMatchesAllowedType(errors, argument.getValue(), allowedArg.getType());
            }
        });
        allowedArgs.forEach((argName, definitionArgument) -> {
            if (isNoNullArgWithoutDefaultValue(definitionArgument)) {
                if (!providedArgs.containsKey(argName)) {
                    errors.add(new DirectiveMissingNonNullArgumentError(element, elementName, directive.getName(), argName));
                }
            }
        });
    }

    private boolean isNoNullArgWithoutDefaultValue(InputValueDefinition definitionArgument) {
        return definitionArgument.getType() instanceof NonNullType && definitionArgument.getDefaultValue() == null;
    }
}
