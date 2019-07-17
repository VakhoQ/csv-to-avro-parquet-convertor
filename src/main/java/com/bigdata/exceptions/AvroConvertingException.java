package com.bigdata.exceptions;

public class AvroConvertingException extends  Exception {
    public AvroConvertingException() {
    }

    public AvroConvertingException(String s) {
        super(s);
    }

    public AvroConvertingException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public AvroConvertingException(Throwable throwable) {
        super(throwable);
    }

    public AvroConvertingException(String s, Throwable throwable, boolean b, boolean b1) {
        super(s, throwable, b, b1);
    }
}
