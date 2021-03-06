//
//  String+Data.swift
//  Reswifq
//
//  Created by Valerio Mazzeo on 26/02/2017.
//  Copyright © 2017 VMLabs Limited. All rights reserved.
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//  See the GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program. If not, see <http://www.gnu.org/licenses/>.
//

import Foundation

public extension String {

    public enum ConversionError: Error {
        case dataConversionError
    }

    public func data(using encoding: Encoding) throws -> Data {

        #if os(Linux)
            /**
             Encoding an empty string on linux returns `nil`, meanwhile on macOS it returns empty `Data`.
             Make sure the behaviour is always the same.
             */
            guard !self.isEmpty else {
                return Data()
            }

            let encodedString: Data? = self.data(using: encoding)
        #else
            let encodedString: Data? = self.data(using: encoding)
        #endif

        guard let data = encodedString else {
            throw ConversionError.dataConversionError
        }
        
        return data
    }
}
