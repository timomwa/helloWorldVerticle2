package com.hello.world.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;

/**
 * All Verticles extend the abstract 
 * class io.vertx.core.AbstractVerticle
 * 
 * This Hello World example creates a simple HTTP server,
 * that will accept connections from the a pre-defined port
 * 
 * @author mwangigikonyo
 *
 */
public class HelloWorldVerticle extends AbstractVerticle {
	
	final int SERVER_PORT = 8787;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		vertx.createHttpServer().requestHandler(
				req -> {
						req
						.response()
						.putHeader("content-type", "text/plain")
						.end("Hello World!\r\nThis is from a Verticle!");
						
		}).listen(SERVER_PORT, http -> {
			
			if (http.succeeded()) {
				startPromise.complete();
				System.out.println(String.format("HTTP server started on port %s", SERVER_PORT));
			} else {
				startPromise.fail(http.cause());
			}
			
		});
	}
}