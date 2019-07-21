package graphql.schema.idl;

import graphql.AssertException;
import graphql.GraphQLError;
import graphql.Internal;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.Directive;
import graphql.language.EnumTypeDefinition;
import graphql.language.EnumValue;
import graphql.language.EnumValueDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.Node;
import graphql.language.NonNullType;
import graphql.language.NullValue;
import graphql.language.ObjectField;
import graphql.language.ObjectValue;
import graphql.language.ScalarTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.language.Value;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.GraphQLScalarType;
import graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static graphql.Assert.assertShouldNeverHappen;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.DUPLICATED_KEYS_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.EXPECTED_ENUM_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.EXPECTED_LIST_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.EXPECTED_NON_NULL_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.EXPECTED_OBJECT_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.EXPECTED_SCALAR_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.MISSING_REQUIRED_FIELD_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.MUST_BE_VALID_ENUM_VALUE_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.NOT_A_VALID_SCALAR_LITERAL_MESSAGE;
import static graphql.schema.idl.errors.DirectiveIllegalArgumentTypeError.UNKNOWN_FIELDS_MESSAGE;

/**
 * Class to check whether a given directive argument value
 * matches a given directive definition.
 *
 */
@Internal
class ArgValueOfAllowedTypeChecker {

    private static final Logger log = LoggerFactory.getLogger(ArgValueOfAllowedTypeChecker.class);

    private final Directive directive;
    private final Node element;
    private final String elementName;
    private final Argument argument;
    private final TypeDefinitionRegistry typeRegistry;
    private final RuntimeWiring runtimeWiring;

    ArgValueOfAllowedTypeChecker(final Directive directive,
                                 final Node element,
                                 final String elementName,
                                 final Argument argument,
                                 final TypeDefinitionRegistry typeRegistry,
                                 final RuntimeWiring runtimeWiring) {
        this.directive = directive;
        this.element = element;
        this.elementName = elementName;
        this.argument = argument;
        this.typeRegistry = typeRegistry;
        this.runtimeWiring = runtimeWiring;
    }

    /**
     * Recursively inspects an argument value given an allowed type.
     * Given the (invalid) SDL below:
     *
     *      directive @myDirective(arg: [[String]] ) on FIELD_DEFINITION
     *
     *      query {
     *          f: String @myDirective(arg: ["A String"])
     *      }
     *
     * it will first check that the `myDirective.arg` type is an array
     * and fail when finding "A String" as it expected a nested array ([[String]]).
     * @param errors validation error collector
     * @param instanceValue directive argument value
     * @param allowedArgType directive definition argument allowed type
     */
    void checkArgValueMatchesAllowedType(List<GraphQLError> errors, Value instanceValue, Type allowedArgType) {
        if (allowedArgType instanceof TypeName) {
            checkArgValueMatchesAllowedTypeName(errors, instanceValue, allowedArgType);
        } else if (allowedArgType instanceof ListType) {
            checkArgValueMatchesAllowedListType(errors, instanceValue, (ListType) allowedArgType);
        } else if (allowedArgType instanceof NonNullType) {
            checkArgValueMatchesAllowedNonNullType(errors, instanceValue, (NonNullType) allowedArgType);
        } else {
            assertShouldNeverHappen("Unsupported Type '%s' was added. ", allowedArgType);
        }
    }

    private void addValidationError(List<GraphQLError> errors, String message, Object... args) {
        errors.add(new DirectiveIllegalArgumentTypeError(element, elementName, directive.getName(), argument.getName(), String.format(message, args)));
    }

    private void checkArgValueMatchesAllowedTypeName(List<GraphQLError> errors, Value instanceValue, Type allowedArgType) {
        if (instanceValue instanceof NullValue) {
            return;
        }

        String allowedTypeName = ((TypeName) allowedArgType).getName();
        TypeDefinition allowedTypeDefinition = typeRegistry.getType(allowedTypeName)
                .orElseThrow(() -> new AssertException("Directive unknown argument type '%s'. This should have been validated before."));

        if (allowedTypeDefinition instanceof ScalarTypeDefinition) {
            checkArgValueMatchesAllowedScalar(errors, instanceValue, allowedTypeName);
        } else if (allowedTypeDefinition instanceof EnumTypeDefinition) {
            checkArgValueMatchesAllowedEnum(errors, instanceValue, (EnumTypeDefinition) allowedTypeDefinition);
        } else if (allowedTypeDefinition instanceof InputObjectTypeDefinition) {
            checkArgValueMatchesAllowedInputType(errors, instanceValue, (InputObjectTypeDefinition) allowedTypeDefinition);
        } else {
            assertShouldNeverHappen("'%s' must be an input type. It is %s instead. ", allowedTypeName, allowedTypeDefinition.getClass());
        }
    }

    private void checkArgValueMatchesAllowedInputType(List<GraphQLError> errors, Value instanceValue, InputObjectTypeDefinition allowedTypeDefinition) {
        if (!(instanceValue instanceof ObjectValue)) {
            addValidationError(errors, EXPECTED_OBJECT_MESSAGE, instanceValue.getClass().getSimpleName());
            return;
        }

        ObjectValue objectValue = ((ObjectValue) instanceValue);
        // duck typing validation, if it looks like the definition
        // then it must be the same type as the definition

        List<ObjectField> fields = objectValue.getObjectFields();
        List<InputValueDefinition> inputValueDefinitions = allowedTypeDefinition.getInputValueDefinitions();

        // check for duplicated fields
        Map<String, Long> fieldsToOccurrenceMap = fields.stream().map(ObjectField::getName)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        if (fieldsToOccurrenceMap.values().stream().anyMatch(count -> count > 1)) {
            addValidationError(errors, DUPLICATED_KEYS_MESSAGE, fieldsToOccurrenceMap.entrySet().stream()
                    .filter(entry -> entry.getValue() > 1)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.joining(",")));
            return;
        }

        // check for unknown fields
        Map<String, InputValueDefinition> nameToInputValueDefMap = inputValueDefinitions.stream()
                .collect(Collectors.toMap(InputValueDefinition::getName, inputValueDef -> inputValueDef));

        List<ObjectField> unknownFields = fields.stream()
                .filter(field -> !nameToInputValueDefMap.containsKey(field.getName()))
                .collect(Collectors.toList());

        if (!unknownFields.isEmpty()) {
            addValidationError(errors, UNKNOWN_FIELDS_MESSAGE,
                    unknownFields.stream()
                            .map(ObjectField::getName)
                            .collect(Collectors.joining(",")),
                    allowedTypeDefinition.getName());
            return;
        }

        // fields to map for easy access
        Map<String, ObjectField> nameToFieldsMap = fields.stream()
                .collect(Collectors.toMap(ObjectField::getName, objectField -> objectField));
        // check each single field with its definition
        inputValueDefinitions.forEach(allowedValueDef -> {
            ObjectField objectField = nameToFieldsMap.get(allowedValueDef.getName());
            checkArgInputObjectValueFieldMatchesAllowedDefinition(errors, objectField, allowedValueDef);
        });
    }

    private void checkArgValueMatchesAllowedEnum(List<GraphQLError> errors, Value instanceValue, EnumTypeDefinition allowedTypeDefinition) {
        if (!(instanceValue instanceof EnumValue)) {
            addValidationError(errors, EXPECTED_ENUM_MESSAGE, instanceValue.getClass().getSimpleName());
            return;
        }

        EnumValue enumValue = ((EnumValue) instanceValue);

        boolean noneMatchAllowedEnumValue = allowedTypeDefinition.getEnumValueDefinitions().stream()
                .noneMatch(enumAllowedValue -> enumAllowedValue.getName().equals(enumValue.getName()));

        if (noneMatchAllowedEnumValue) {
            addValidationError(errors, MUST_BE_VALID_ENUM_VALUE_MESSAGE, enumValue.getName(), allowedTypeDefinition.getEnumValueDefinitions().stream()
                            .map(EnumValueDefinition::getName)
                            .collect(Collectors.joining(",")));
        }
    }

    private void checkArgValueMatchesAllowedScalar(List<GraphQLError> errors, Value instanceValue, String allowedTypeName) {
        if (instanceValue instanceof ArrayValue
                || instanceValue instanceof EnumValue
                || instanceValue instanceof ObjectValue) {
            addValidationError(errors, EXPECTED_SCALAR_MESSAGE, instanceValue.getClass().getSimpleName());
            return;
        }

        GraphQLScalarType scalarType = runtimeWiring.getScalars().get(allowedTypeName);
        // scalarType will always be present as
        // scalar implementation validation has been performed earlier
        if (!isArgumentValueScalarLiteral(scalarType, instanceValue)) {
            addValidationError(errors, NOT_A_VALID_SCALAR_LITERAL_MESSAGE, allowedTypeName);
        }
    }

    private void checkArgInputObjectValueFieldMatchesAllowedDefinition(List<GraphQLError> errors, ObjectField objectField, InputValueDefinition allowedValueDef) {

        if (objectField != null) {
            checkArgValueMatchesAllowedType(errors, objectField.getValue(), allowedValueDef.getType());
            return;
        }

        // check if field definition is required and has no default value
        if (allowedValueDef.getType() instanceof NonNullType && allowedValueDef.getDefaultValue() == null) {
            addValidationError(errors, MISSING_REQUIRED_FIELD_MESSAGE, allowedValueDef.getName());
        }

        // other cases are
        // - field definition is marked as non-null but has a default value, so the default value can be used
        // - field definition is nullable hence null can be used
    }

    private void checkArgValueMatchesAllowedNonNullType(List<GraphQLError> errors, Value instanceValue, NonNullType allowedArgType) {
        if (instanceValue instanceof NullValue) {
            addValidationError(errors, EXPECTED_NON_NULL_MESSAGE);
            return;
        }

        Type unwrappedAllowedType = allowedArgType.getType();
        checkArgValueMatchesAllowedType(errors, instanceValue, unwrappedAllowedType);
    }

    private void checkArgValueMatchesAllowedListType(List<GraphQLError> errors, Value instanceValue, ListType allowedArgType) {
        if (instanceValue instanceof NullValue) {
            return;
        }

        if (!(instanceValue instanceof ArrayValue)) {
            addValidationError(errors, EXPECTED_LIST_MESSAGE, instanceValue.getClass().getSimpleName());
            return;
        }

        ArrayValue arrayValue = ((ArrayValue) instanceValue);
        Type unwrappedAllowedType = allowedArgType.getType();

        // validate each instance value in the list, all instances must match for the list to match
        arrayValue.getValues().forEach(value -> checkArgValueMatchesAllowedType(errors, value, unwrappedAllowedType));
    }

    private boolean isArgumentValueScalarLiteral(GraphQLScalarType scalarType, Value instanceValue) {
        try {
            scalarType.getCoercing().parseLiteral(instanceValue);
            return true;
        } catch (CoercingParseLiteralException ex) {
            log.debug("Attempted parsing literal into '{}' but got the following error: ", scalarType.getName(), ex);
            return false;
        }
    }
}
