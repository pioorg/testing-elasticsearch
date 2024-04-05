package testing_elasticsearch;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import javax.net.ssl.SSLContext;

public class ElasticsearchRestClientBuilder {

    private HttpHost httpHost;
    private SSLContext sslContext;
    private UsernamePasswordCredentials credentials;
    private BasicHeader authHeader;

    public ElasticsearchRestClientBuilder withHttpHost(String host) {
        this.httpHost = new HttpHost(host);
        return this;
    }

    public ElasticsearchRestClientBuilder withHttpHost(HttpHost host) {
        this.httpHost = host;
        return this;
    }

    public ElasticsearchRestClientBuilder withSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    public ElasticsearchRestClientBuilder withUsernameAndPassword(String username, String password) {
        this.credentials = new UsernamePasswordCredentials(username, password);
        return this;
    }

    public ElasticsearchRestClientBuilder withApiKey(String apiKey) {
        this.authHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "ApiKey " + apiKey);
        return this;
    }

    public RestClient build() {
        org.elasticsearch.client.RestClientBuilder builder = RestClient.builder(httpHost);
        if (sslContext != null || credentials != null) {

            builder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    if (sslContext != null) {
                        httpClientBuilder.setSSLContext(sslContext);
                    }
                    if (credentials != null) {
                        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY, credentials);
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                    return httpClientBuilder;
                }
            );
        }
        if (authHeader != null) {
            builder.setDefaultHeaders(new Header[]{authHeader});
        }
        return builder.build();
    }
}
