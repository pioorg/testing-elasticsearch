/*
 *  Copyright (C) 2024 Piotr Przybył
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
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
