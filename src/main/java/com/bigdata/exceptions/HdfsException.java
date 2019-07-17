package com.bigdata.exceptions;

public class HdfsException extends  Exception {
    public HdfsException() {
    }

    public HdfsException(String s) {
        super(s);
    }

    public HdfsException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public HdfsException(Throwable throwable) {
        super(throwable);
    }

    public HdfsException(String s, Throwable throwable, boolean b, boolean b1) {
        super(s, throwable, b, b1);
    }
}

