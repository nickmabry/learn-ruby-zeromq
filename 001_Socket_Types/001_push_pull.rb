require 'rubygems'
require 'ffi-rzmq'
Thread.abort_on_exception = true

# PUSH and PULL sockets work together to load balance messages going one way.
# Multiple PULL sockets connected to a PUSH each receive messages from the PUSH.
# ZeroMQ automatically load balances the messages between all pull sockets.
#
# We're going to build a simple load balanced message system that looks like this:
#
#                         push_sock
#                         /       \
#                  pull_sock1   pull_sock2
#
# Each socket will get its own thread, so you'll see them run simultanously

#The context creates all ZeroMQ sockets.
#It's thread safe, and multiple contexts can exist within an application.
ctx = ZMQ::Context.new(1) 

push_thread = Thread.new do
  #Here we're creating our first socket. Sockets should not be shared among threads.
  push_sock = ctx.socket(ZMQ::PUSH)
  push_sock.bind('tcp://127.0.0.1:2200')
  
  7.times do |i|
    msg = "#{i + 1} Potato"
    puts "Sending #{msg}"
    #This will block till a PULL socket connects`
    push_sock.send_string(msg)
    
    #Lets wait a second between messages
    sleep 1
  end

  #Close the socket immediately
  push_sock.close
end

#Here we create two pull sockets, you'll see an alternating pattern
#of message reception between these two sockets
pull_threads = []
2.times do |i|
  pull_threads << Thread.new do
    pull_sock = ctx.socket(ZMQ::PULL)
    sleep 3
    puts "Pull #{i} connecting"
    pull_sock.connect('tcp://127.0.0.1:2200')
    
    begin
      #Here we receive messages
      while message = pull_sock.recv_string do
        puts "Pull#{i}: I received a message '#{message}'"

        # Stop receiving if this is our socket's last message
        if "6 Potato" == message or "7 Potato" == message
          puts "Pull#{i} closing"
          break;
        end
      end
    #On termination sockets raise an error, lets handle this nicely
    #Later, we'll learn how to use polling to handle this type of situation
    #more gracefully
    rescue ZMQ::SocketError => e
      puts "Socket terminated: #{e.message}"
    end

    pull_sock.close()
  end
end

#Wait till we're done pushing messages
push_thread.join

#Wait till the pull threads finish executing
pull_threads.each {|t| t.join}

#Terminate the context after all sockets are closed
ctx.terminate

puts "Done!"
