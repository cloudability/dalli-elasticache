require 'dalli'
require 'resolv'
require 'socket'
require 'dalli/elasticache/version'

module Dalli
  class ElastiCache
    attr_accessor :config_host, :config_port, :options

    def initialize(config_endpoint, options={})
      @config_host, @config_port = config_endpoint.split(':')
      @config_port ||= 11211
      @options = options

    end

    def client
      Dalli::Client.new(servers, options)
    end

    def refresh
      # Reset data
      @data = nil
      data

      self
    end

    def config_get_cluster
      s = TCPSocket.new(config_host, config_port)
      s.puts "config get cluster\r\n"
      data = []
      while (line = s.gets) != "END\r\n"
        break if line == "ERROR\r\n"
        data << line
      end

      s.close
      data
    end

    def data
      return @data if @data
      raw_data = config_get_cluster

      # We didn't get raw data back so we're assuming it's not in clustered
      # mode. We'll synthesize our own data out of that.
      if raw_data.empty?
        instance = { :host => config_host, :ip => resolved_config_host, :port => config_port }

        # TODO: Version should be...anything else? Is it significant?
        @data = { :version => 1, :instances => [instance] }
      else
        version = raw_data[1].to_i
        instance_data = raw_data[2].split(/\s+/)
        instances = instance_data.map{ |raw| host, ip, port = raw.split('|'); {:host => host, :ip => ip, :port => port} }
        @data = { :version => version, :instances => instances }
      end
    end

    def version
      data[:version]
    end

    def servers
      data[:instances].map{ |i| "#{i[:ip]}:#{i[:port]}" }
    end

    def resolved_config_host
      # If it's already an IP address, let's not worry 'bout it.
      return config_host.strip if ipv4?(config_host)

      # Try to find an IPv4 addr for this host!
      hosts = Resolv.getaddresses(config_host)
      hosts.detect { |str| ipv4?(str) } || config_host
    end

    def ipv4?(str)
      str.strip =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/
    end
  end
end
