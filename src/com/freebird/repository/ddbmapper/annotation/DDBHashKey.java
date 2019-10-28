package com.freebird.repository.ddbmapper.annotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Documented
@Retention(RUNTIME)
@Target(FIELD)
public @interface DDBHashKey {
	
	public enum KEY_GEN {NONE,UUID,MONTH,DAY,NUM,NUM_STR};
	
	String name();	
	KEY_GEN gen() default KEY_GEN.NONE;
}
