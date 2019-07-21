package graphql.schema.idl.errors;

import graphql.language.Node;

import static java.lang.String.format;

public class DirectiveIllegalArgumentTypeError extends BaseError {

    public static final String DUPLICATED_KEYS_MESSAGE = "Argument value object keys [%s] appear more than once.";
    public static final String UNKNOWN_FIELDS_MESSAGE = "Fields ['%s'] not present in type '%s'.";
    public static final String EXPECTED_ENUM_MESSAGE = "Argument value is of type '%s', expected an enum value.";
    public static final String MUST_BE_VALID_ENUM_VALUE_MESSAGE = "Argument value '%s' doesn't match any of the allowed enum values ['%s']";
    public static final String EXPECTED_SCALAR_MESSAGE = "Argument value is of type '%s', expected a scalar.";
    public static final String NOT_A_VALID_SCALAR_LITERAL_MESSAGE = "Argument value is not a valid value of scalar '%s'.";
    public static final String MISSING_REQUIRED_FIELD_MESSAGE = "Missing required field '%s'.";
    public static final String EXPECTED_NON_NULL_MESSAGE = "Argument value is 'null', expected a non-null value.";
    public static final String EXPECTED_LIST_MESSAGE = "Argument value is '%s', expected a list value.";
    public static final String EXPECTED_OBJECT_MESSAGE = "Argument value is of type '%s', expected an Object value.";

    public DirectiveIllegalArgumentTypeError(Node element, String elementName, String directiveName, String argumentName, String detailedMessaged) {
        super(element, mkDirectiveIllegalArgumentTypeErrorMessage(element, elementName, directiveName, argumentName, detailedMessaged));
    }

    static String mkDirectiveIllegalArgumentTypeErrorMessage(Node element,
                                                             String elementName,
                                                             String directiveName,
                                                             String argumentName,
                                                             String detailedMessage) {
        return format("'%s' %s uses an illegal value for the argument '%s' on directive '%s'. %s",
                elementName, BaseError.lineCol(element), argumentName, directiveName, detailedMessage);
    }
}
