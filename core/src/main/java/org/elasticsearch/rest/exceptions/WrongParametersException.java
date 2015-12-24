package org.elasticsearch.rest.exceptions;

/**
 * Created by psm on 24/12/15.
 */
public class WrongParametersException extends Exception {
    public static final WrongParametersException INSTANCE = new WrongParametersException();
}
