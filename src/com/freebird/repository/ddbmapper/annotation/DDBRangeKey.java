package com.freebird.repository.ddbmapper.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.freebird.repository.ddbmapper.annotation.DDBHashKey.KEY_GEN;

@Documented
@Retention(RUNTIME)
@Target(FIELD)
public @interface DDBRangeKey {
    
   //public enum KEY_GEN {NONE,UUID,MOUNTH,DAY,NUM,NUM_STR};  用DDBHashKey.KEY_GEN的
    
    String name();
    String prefix() default "";
    boolean required() default false;    
    KEY_GEN gen() default KEY_GEN.NONE;
}
