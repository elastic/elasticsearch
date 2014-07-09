package org.elasticsearch.plugins;

import sun.misc.BASE64Encoder;

public class BasicAuthCredentials {

    public static BasicAuthCredentials NONE = new BasicAuthCredentials();

    private String username;
    private String password;

    private BasicAuthCredentials() {
    }

    public BasicAuthCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String encodedAuthorization() {
        if(isEmpty()) {
            return "";
        }

        BASE64Encoder enc = new sun.misc.BASE64Encoder();
        String userpassword = username + ":" + password;
        return enc.encode( userpassword.getBytes() );
    }

    public boolean isEmpty() {
        return username==null || password==null;
    }
}
