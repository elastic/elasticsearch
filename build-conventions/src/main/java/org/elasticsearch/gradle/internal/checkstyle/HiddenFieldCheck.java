/*
 * @notice
 * checkstyle: Checks Java source code for adherence to a set of rules.
 * Copyright (C) 2001-2021 the original author or authors.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.FileStatefulCheck;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.Scope;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import com.puppycrawl.tools.checkstyle.utils.CheckUtil;
import com.puppycrawl.tools.checkstyle.utils.ScopeUtil;
import com.puppycrawl.tools.checkstyle.utils.TokenUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This is a copy of Checkstyle's {@link com.puppycrawl.tools.checkstyle.checks.coding.HiddenFieldCheck}. See
 * the original class for full documentation.
 */
@FileStatefulCheck
public class HiddenFieldCheck extends AbstractCheck {

    /**
     * A key is pointing to the warning message text in "messages.properties"
     * file.
     */
    public static final String MSG_KEY = "hidden.field";

    /**
     * Stack of sets of field names,
     * one for each class of a set of nested classes.
     */
    private FieldFrame frame;

    /** Define the RegExp for names of variables and parameters to ignore. */
    private Pattern ignoreFormat;

    /**
     * Allow to ignore the parameter of a property setter method.
     */
    private boolean ignoreSetter;

    /**
     * Allow to expand the definition of a setter method to include methods
     * that return the class' instance.
     */
    private boolean setterCanReturnItsClass;

    /** Control whether to ignore constructor parameters. */
    private boolean ignoreConstructorParameter;

    /** Control whether to ignore variables in constructor bodies. */
    private boolean ignoreConstructorBody;

    /** Control whether to ignore parameters of abstract methods. */
    private boolean ignoreAbstractMethods;

    /** If set, specifies a regex of method names that should be ignored */
    private String ignoreMethodNames;

    /** If set, specifies a regex of constructor names that should be ignored */
    private String ignoreConstructorMethods;

    /** Control the minimal amount of lines in method to allow shadowed variables .*/
    private int minLineCount = -1;

    @Override
    public int[] getDefaultTokens() {
        return getAcceptableTokens();
    }

    @Override
    public int[] getAcceptableTokens() {
        return new int[] {
            TokenTypes.VARIABLE_DEF,
            TokenTypes.PARAMETER_DEF,
            TokenTypes.CLASS_DEF,
            TokenTypes.ENUM_DEF,
            TokenTypes.ENUM_CONSTANT_DEF,
            TokenTypes.PATTERN_VARIABLE_DEF,
            TokenTypes.LAMBDA,
            TokenTypes.RECORD_DEF,
            TokenTypes.RECORD_COMPONENT_DEF, };
    }

    @Override
    public int[] getRequiredTokens() {
        return new int[] { TokenTypes.CLASS_DEF, TokenTypes.ENUM_DEF, TokenTypes.ENUM_CONSTANT_DEF, TokenTypes.RECORD_DEF, };
    }

    @Override
    public void beginTree(DetailAST rootAST) {
        frame = new FieldFrame(null, true, null);
    }

    @Override
    public void visitToken(DetailAST ast) {
        final int type = ast.getType();
        switch (type) {
            case TokenTypes.VARIABLE_DEF:
            case TokenTypes.PARAMETER_DEF:
            case TokenTypes.PATTERN_VARIABLE_DEF:
            case TokenTypes.RECORD_COMPONENT_DEF:
                processVariable(ast);
                break;
            case TokenTypes.LAMBDA:
                processLambda(ast);
                break;
            default:
                visitOtherTokens(ast, type);
        }
    }

    /**
     * Process a lambda token.
     * Checks whether a lambda parameter shadows a field.
     * Note, that when parameter of lambda expression is untyped,
     * ANTLR parses the parameter as an identifier.
     *
     * @param ast the lambda token.
     */
    private void processLambda(DetailAST ast) {
        final DetailAST firstChild = ast.getFirstChild();
        if (firstChild != null && firstChild.getType() == TokenTypes.IDENT) {
            final String untypedLambdaParameterName = firstChild.getText();
            if (frame.containsStaticField(untypedLambdaParameterName) || isInstanceField(firstChild, untypedLambdaParameterName)) {
                log(firstChild, MSG_KEY, untypedLambdaParameterName);
            }
        }
    }

    /**
     * Called to process tokens other than {@link TokenTypes#VARIABLE_DEF}
     * and {@link TokenTypes#PARAMETER_DEF}.
     *
     * @param ast token to process
     * @param type type of the token
     */
    private void visitOtherTokens(DetailAST ast, int type) {
        // A more thorough check of enum constant class bodies is
        // possible (checking for hidden fields against the enum
        // class body in addition to enum constant class bodies)
        // but not attempted as it seems out of the scope of this
        // check.
        final DetailAST typeMods = ast.findFirstToken(TokenTypes.MODIFIERS);
        final boolean isStaticInnerType = typeMods != null && typeMods.findFirstToken(TokenTypes.LITERAL_STATIC) != null;
        final String frameName;

        if (type == TokenTypes.CLASS_DEF || type == TokenTypes.ENUM_DEF) {
            frameName = ast.findFirstToken(TokenTypes.IDENT).getText();
        } else {
            frameName = null;
        }
        final FieldFrame newFrame = new FieldFrame(frame, isStaticInnerType, frameName);

        // add fields to container
        final DetailAST objBlock = ast.findFirstToken(TokenTypes.OBJBLOCK);
        // enum constants may not have bodies
        if (objBlock != null) {
            DetailAST child = objBlock.getFirstChild();
            while (child != null) {
                if (child.getType() == TokenTypes.VARIABLE_DEF) {
                    final String name = child.findFirstToken(TokenTypes.IDENT).getText();
                    final DetailAST mods = child.findFirstToken(TokenTypes.MODIFIERS);
                    if (mods.findFirstToken(TokenTypes.LITERAL_STATIC) == null) {
                        newFrame.addInstanceField(name);
                    } else {
                        newFrame.addStaticField(name);
                    }
                }
                child = child.getNextSibling();
            }
        }
        if (ast.getType() == TokenTypes.RECORD_DEF) {
            final DetailAST recordComponents = ast.findFirstToken(TokenTypes.RECORD_COMPONENTS);

            // For each record component definition, we will add it to this frame.
            TokenUtil.forEachChild(recordComponents, TokenTypes.RECORD_COMPONENT_DEF, node -> {
                final String name = node.findFirstToken(TokenTypes.IDENT).getText();
                newFrame.addInstanceField(name);
            });
        }
        // push container
        frame = newFrame;
    }

    @Override
    public void leaveToken(DetailAST ast) {
        if (ast.getType() == TokenTypes.CLASS_DEF
            || ast.getType() == TokenTypes.ENUM_DEF
            || ast.getType() == TokenTypes.ENUM_CONSTANT_DEF
            || ast.getType() == TokenTypes.RECORD_DEF) {
            // pop
            frame = frame.getParent();
        }
    }

    /**
     * Process a variable token.
     * Check whether a local variable or parameter shadows a field.
     * Store a field for later comparison with local variables and parameters.
     *
     * @param ast the variable token.
     */
    private void processVariable(DetailAST ast) {
        if (ScopeUtil.isInInterfaceOrAnnotationBlock(ast) == false
            && CheckUtil.isReceiverParameter(ast) == false
            && (ScopeUtil.isLocalVariableDef(ast)
                || ast.getType() == TokenTypes.PARAMETER_DEF
                || ast.getType() == TokenTypes.PATTERN_VARIABLE_DEF)) {
            // local variable or parameter. Does it shadow a field?
            final DetailAST nameAST = ast.findFirstToken(TokenTypes.IDENT);
            final String name = nameAST.getText();

            if ((frame.containsStaticField(name) || isInstanceField(ast, name))
                && isMatchingRegexp(name) == false
                && isIgnoredParam(ast, name) == false
                && isIgnoredVariable(ast, name) == false) {
                log(nameAST, MSG_KEY, name);
            }
        }
    }

    /**
     * Checks whether method or constructor parameter is ignored.
     *
     * @param ast the parameter token.
     * @param name the parameter name.
     * @return true if parameter is ignored.
     */
    private boolean isIgnoredParam(DetailAST ast, String name) {
        return isVariableInIgnoredMethod(ast, name)
            || isIgnoredSetterParam(ast, name)
            || isIgnoredConstructorParam(ast)
            || isIgnoredParamOfAbstractMethod(ast);
    }

    private boolean isIgnoredVariable(DetailAST ast, String name) {
        return isVariableInConstructorBody(ast, name) ||
            isVariableInIgnoredConstructor(ast, name);
    }

    /**
     * Check for instance field.
     *
     * @param ast token
     * @param name identifier of token
     * @return true if instance field
     */
    private boolean isInstanceField(DetailAST ast, String name) {
        return isInStatic(ast) == false && frame.containsInstanceField(name);
    }

    /**
     * Check name by regExp.
     *
     * @param name string value to check
     * @return true is regexp is matching
     */
    private boolean isMatchingRegexp(String name) {
        return ignoreFormat != null && ignoreFormat.matcher(name).find();
    }

    /**
     * Determines whether an AST node is in a static method or static
     * initializer.
     *
     * @param ast the node to check.
     * @return true if ast is in a static method or a static block;
     */
    private static boolean isInStatic(DetailAST ast) {
        DetailAST parent = ast.getParent();
        boolean inStatic = false;

        while (parent != null && inStatic == false) {
            if (parent.getType() == TokenTypes.STATIC_INIT) {
                inStatic = true;
            } else if (parent.getType() == TokenTypes.METHOD_DEF && ScopeUtil.isInScope(parent, Scope.ANONINNER) == false
                || parent.getType() == TokenTypes.VARIABLE_DEF) {
                    final DetailAST mods = parent.findFirstToken(TokenTypes.MODIFIERS);
                    inStatic = mods.findFirstToken(TokenTypes.LITERAL_STATIC) != null;
                    break;
                } else {
                    parent = parent.getParent();
                }
        }
        return inStatic;
    }

    /**
     * Decides whether to ignore an AST node that is the parameter of a
     * setter method, where the property setter method for field 'xyz' has
     * name 'setXyz', one parameter named 'xyz', and return type void
     * (default behavior) or return type is name of the class in which
     * such method is declared (allowed only if
     * {@link #setSetterCanReturnItsClass(boolean)} is called with
     * value <em>true</em>).
     *
     * @param ast the AST to check.
     * @param name the name of ast.
     * @return true if ast should be ignored because check property
     *     ignoreSetter is true and ast is the parameter of a setter method.
     */
    private boolean isIgnoredSetterParam(DetailAST ast, String name) {
        boolean isIgnoredSetterParam = false;
        if (ignoreSetter && ast.getType() == TokenTypes.PARAMETER_DEF) {
            final DetailAST parametersAST = ast.getParent();
            final DetailAST methodAST = parametersAST.getParent();
            if (parametersAST.getChildCount() == 1 && methodAST.getType() == TokenTypes.METHOD_DEF && isSetterMethod(methodAST, name)) {
                isIgnoredSetterParam = true;
            }
        }
        return isIgnoredSetterParam;
    }

    /**
     * Determine if a specific method identified by methodAST and a single
     * variable name aName is a setter. This recognition partially depends
     * on setterCanReturnItsClass property.
     *
     * @param aMethodAST AST corresponding to a method call
     * @param aName name of single parameter of this method.
     * @return true of false indicating of method is a setter or not.
     */
    private boolean isSetterMethod(DetailAST aMethodAST, String aName) {
        final String methodName = aMethodAST.findFirstToken(TokenTypes.IDENT).getText();
        boolean isSetterMethod = false;

        // ES also allows setters with the same name as a property, and builder-style settings that start with "with".
        final List<String> possibleSetterNames = List.of(
            "set" + capitalize(aName, true),
            "set" + capitalize(aName, false),
            "with" + capitalize(aName, true),
            "with" + capitalize(aName, false),
            aName
        );

        if (possibleSetterNames.contains(methodName)) {
            // method name did match set${Name}(${anyType} ${aName})
            // where ${Name} is capitalized version of ${aName}
            // therefore this method is potentially a setter
            final DetailAST typeAST = aMethodAST.findFirstToken(TokenTypes.TYPE);
            final String returnType = typeAST.getFirstChild().getText();

            // The method is named `setFoo`, `withFoo`, or just `foo` and returns void
            final boolean returnsVoid = typeAST.findFirstToken(TokenTypes.LITERAL_VOID) != null;

            // Or the method is named as above, and returns the class type or a builder type.
            // It ought to be possible to see if we're in a `${returnType}.Builder`, but for some reason the parse
            // tree has `returnType` as `.` when the current class is `Builder` so instead assume that a class called `Builder` is OK.
            final boolean returnsSelf = setterCanReturnItsClass && frame.isEmbeddedIn(returnType);

            final boolean returnsBuilder = setterCanReturnItsClass
                && (frame.isEmbeddedIn(returnType + "Builder") || (frame.isEmbeddedIn("Builder")));

            if (returnsVoid || returnsSelf || returnsBuilder) {
                isSetterMethod = true;
            }
        }

        return isSetterMethod;
    }

    /**
     * Capitalizes a given property name the way we expect to see it in
     * a setter name.
     *
     * @param name a property name
     * @return capitalized property name
     */
    private static String capitalize(final String name, boolean javaBeanCompliant) {
        String setterName = name;
        // we should not capitalize the first character if the second
        // one is a capital one, since according to JavaBeans spec
        // setXYzz() is a setter for XYzz property, not for xYzz one.
        // @pugnascotia: this is unhelpful in the Elasticsearch codebase. We have e.g. xContent -> setXContent, or nNeighbors -> nNeighbors.
        if (name.length() == 1 || (javaBeanCompliant == false || Character.isUpperCase(name.charAt(1)) == false)) {
            setterName = name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1);
        }
        return setterName;
    }

    /**
     * Decides whether to ignore an AST node that is the parameter of a
     * constructor.
     *
     * @param ast the AST to check.
     * @return true if ast should be ignored because check property
     *     ignoreConstructorParameter is true and ast is a constructor parameter.
     */
    private boolean isIgnoredConstructorParam(DetailAST ast) {
        boolean result = false;
        if (ignoreConstructorParameter && ast.getType() == TokenTypes.PARAMETER_DEF) {
            final DetailAST parametersAST = ast.getParent();
            final DetailAST constructorAST = parametersAST.getParent();
            result = constructorAST.getType() == TokenTypes.CTOR_DEF;
        }
        return result;
    }

    /**
     * Decides whether to ignore an AST node that is the parameter of an
     * abstract method.
     *
     * @param ast the AST to check.
     * @return true if ast should be ignored because check property
     *     ignoreAbstractMethods is true and ast is a parameter of abstract methods.
     */
    private boolean isIgnoredParamOfAbstractMethod(DetailAST ast) {
        boolean result = false;
        if (ignoreAbstractMethods && ast.getType() == TokenTypes.PARAMETER_DEF) {
            final DetailAST method = ast.getParent().getParent();
            if (method.getType() == TokenTypes.METHOD_DEF) {
                final DetailAST mods = method.findFirstToken(TokenTypes.MODIFIERS);
                result = mods.findFirstToken(TokenTypes.ABSTRACT) != null;
            }
        }
        return result;
    }

    /**
     * Decides whether to ignore an AST node that is witin a method that ought to be ignored.
     * This is either because:
     *
     * <ul>
     *   <li>The method name matches the {@link #ignoreMethodNames} regex, if set.</li>
     *   <li>The method's line length is less that or equal to the {@link #minLineCount}</li>
     * </ul>
     *
     * @param ast the AST to check
     * @return true is the ast node should be ignored
     */
    private boolean isVariableInIgnoredMethod(DetailAST ast, String name) {
        boolean result = false;
        if (ignoreMethodNames != null && (ast.getType() == TokenTypes.PARAMETER_DEF || ast.getType() == TokenTypes.VARIABLE_DEF)) {
            DetailAST method = ast.getParent();
            while (method != null && method.getType() != TokenTypes.METHOD_DEF) {
                method = method.getParent();
            }
            if (method != null && method.getType() == TokenTypes.METHOD_DEF) {
                final String methodName = method.findFirstToken(TokenTypes.IDENT).getText();
                result = methodName.matches(ignoreMethodNames) || getMethodsNumberOfLine(method) <= this.minLineCount;
            }
        }
        return result;
    }

    private boolean isVariableInConstructorBody(DetailAST ast, String name) {
        boolean result = false;

        if (ignoreConstructorBody && ast.getType() == TokenTypes.VARIABLE_DEF) {
            DetailAST method = ast.getParent();
            while (method != null && method.getType() != TokenTypes.CTOR_DEF) {
                method = method.getParent();
            }
            result = method != null && method.getType() == TokenTypes.CTOR_DEF;
        }

        return result;
    }

    private boolean isVariableInIgnoredConstructor(DetailAST ast, String name) {
        boolean result = false;

        if (ignoreConstructorBody && ast.getType() == TokenTypes.VARIABLE_DEF) {
            DetailAST method = ast.getParent();
            while (method != null && method.getType() != TokenTypes.LITERAL_NEW) {
                method = method.getParent();
            }
            if (method != null) {
                final String ctorName = method.findFirstToken(TokenTypes.IDENT).getText();
                result = ctorName.matches(this.ignoreConstructorMethods);
            }
        }

        return result;
    }

    /**
     * Setter to define the RegExp for names of variables and parameters to ignore.
     *
     * @param pattern a pattern.
     */
    public void setIgnoreFormat(Pattern pattern) {
        ignoreFormat = pattern;
    }

    /**
     * Setter to allow to ignore the parameter of a property setter method.
     *
     * @param ignoreSetter decide whether to ignore the parameter of
     *     a property setter method.
     */
    public void setIgnoreSetter(boolean ignoreSetter) {
        this.ignoreSetter = ignoreSetter;
    }

    /**
     * Setter to allow to expand the definition of a setter method to include methods
     * that return the class' instance.
     *
     * @param aSetterCanReturnItsClass if true then setter can return
     *        either void or class in which it is declared. If false then
     *        in order to be recognized as setter method (otherwise
     *        already recognized as a setter) must return void.  Later is
     *        the default behavior.
     */
    public void setSetterCanReturnItsClass(boolean aSetterCanReturnItsClass) {
        setterCanReturnItsClass = aSetterCanReturnItsClass;
    }

    /**
     * Setter to control whether to ignore constructor parameters.
     *
     * @param ignoreConstructorParameter decide whether to ignore
     *     constructor parameters.
     */
    public void setIgnoreConstructorParameter(boolean ignoreConstructorParameter) {
        this.ignoreConstructorParameter = ignoreConstructorParameter;
    }

    /**
     * Setter to control whether to ignore parameters of abstract methods.
     *
     * @param ignoreAbstractMethods decide whether to ignore
     *     parameters of abstract methods.
     */
    public void setIgnoreAbstractMethods(boolean ignoreAbstractMethods) {
        this.ignoreAbstractMethods = ignoreAbstractMethods;
    }

    public void setIgnoreMethodNames(String ignoreMethodNames) {
        this.ignoreMethodNames = ignoreMethodNames;
    }

    public void setIgnoreConstructorBody(boolean ignoreConstructorBody) {
        this.ignoreConstructorBody = ignoreConstructorBody;
    }

    public void setIgnoreConstructorMethods(String ignoreConstructorMethods) {
        this.ignoreConstructorMethods = ignoreConstructorMethods;
    }

    public void setMinLineCount(int minLineCount) {
        this.minLineCount = minLineCount;
    }

    /**
     * Holds the names of static and instance fields of a type.
     */
    private static class FieldFrame {

        /** Name of the frame, such name of the class or enum declaration. */
        private final String frameName;

        /** Is this a static inner type. */
        private final boolean staticType;

        /** Parent frame. */
        private final FieldFrame parent;

        /** Set of instance field names. */
        private final Set<String> instanceFields = new HashSet<>();

        /** Set of static field names. */
        private final Set<String> staticFields = new HashSet<>();

        /**
         * Creates new frame.
         *
         * @param parent parent frame.
         * @param staticType is this a static inner type (class or enum).
         * @param frameName name associated with the frame, which can be a
         */
        /* package */ FieldFrame(FieldFrame parent, boolean staticType, String frameName) {
            this.parent = parent;
            this.staticType = staticType;
            this.frameName = frameName;
        }

        /**
         * Adds an instance field to this FieldFrame.
         *
         * @param field  the name of the instance field.
         */
        public void addInstanceField(String field) {
            instanceFields.add(field);
        }

        /**
         * Adds a static field to this FieldFrame.
         *
         * @param field  the name of the instance field.
         */
        public void addStaticField(String field) {
            staticFields.add(field);
        }

        /**
         * Determines whether this FieldFrame contains an instance field.
         *
         * @param field the field to check.
         * @return true if this FieldFrame contains instance field field.
         */
        public boolean containsInstanceField(String field) {
            return instanceFields.contains(field) || parent != null && staticType == false && parent.containsInstanceField(field);
        }

        /**
         * Determines whether this FieldFrame contains a static field.
         *
         * @param field the field to check.
         * @return true if this FieldFrame contains static field field.
         */
        public boolean containsStaticField(String field) {
            return staticFields.contains(field) || parent != null && parent.containsStaticField(field);
        }

        /**
         * Getter for parent frame.
         *
         * @return parent frame.
         */
        public FieldFrame getParent() {
            return parent;
        }

        /**
         * Check if current frame is embedded in class or enum with
         * specific name.
         *
         * @param classOrEnumName name of class or enum that we are looking
         *     for in the chain of field frames.
         *
         * @return true if current frame is embedded in class or enum
         *     with name classOrNameName
         */
        private boolean isEmbeddedIn(String classOrEnumName) {
            FieldFrame currentFrame = this;
            boolean isEmbeddedIn = false;
            while (currentFrame != null) {
                if (Objects.equals(currentFrame.frameName, classOrEnumName)) {
                    isEmbeddedIn = true;
                    break;
                }
                currentFrame = currentFrame.parent;
            }
            return isEmbeddedIn;
        }

    }

    private static int getMethodsNumberOfLine(DetailAST methodDef) {
        final int numberOfLines;
        final DetailAST lcurly = methodDef.getLastChild();
        final DetailAST rcurly = lcurly.getLastChild();

        if (lcurly.getFirstChild() == rcurly) {
            numberOfLines = 1;
        }
        else {
            numberOfLines = rcurly.getLineNo() - lcurly.getLineNo() - 1;
        }
        return numberOfLines;
    }

}
