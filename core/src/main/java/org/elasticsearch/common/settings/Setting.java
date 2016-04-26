package org.elasticsearch.common.settings;


/**
 * Created by Fabio Torchetti on 4/26/16.
 */

public class Setting {

    private String value;
    private Boolean nullValue;
    private Boolean defined;

    private void setNullValue(String value) {
        if (value == null) {
            nullValue = true;
            return;
        }
        nullValue = false;
    }

    public Setting(String value) {
        this.value = value;
        defined = true;
        setNullValue(value);
    }

    public Setting() {
        value = null;
        defined = false;
        nullValue = true;
    }

    public String get() {
        return value;
    }

    public void set(String value) {
        this.value = value;
        defined = true;
        setNullValue(value);
    }

    public Boolean isNull() {
        return nullValue;
    }

    public Boolean isDefined() {
        return defined;
    }

    /**
     * Check for null value.
     * @return true if value is either null or a Setting with null value.
     */
    public static Boolean isNullValue(Object value) {
        if (value instanceof Setting) {
            return ((Setting) value).isNull();
        }
        return value == null;
    }

}
